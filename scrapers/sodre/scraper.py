#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SODRÉ SANTORO - SCRAPER CORRIGIDO
✅ Espera adaptativa por seção (sucatas precisa mais tempo)
✅ Múltiplas tentativas antes de desistir
✅ Detecta quando API retorna dados vazios vs dados reais
"""

import asyncio
import sys
import json
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict
from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from supabase_client import SupabaseClient
except:
    SupabaseClient = None


class SodreScraperFixed:
    """Scraper Sodré com espera adaptativa"""
    
    def __init__(self, debug=False):
        self.source = 'sodre'
        self.base_url = 'https://www.sodresantoro.com.br'
        self.debug = debug
        
        # ✅ CONFIGURAÇÃO DE ESPERA POR SEÇÃO
        self.section_config = {
            'veiculos': {'wait_time': 7, 'max_retries': 3},
            'imoveis': {'wait_time': 7, 'max_retries': 3},
            'materiais': {'wait_time': 7, 'max_retries': 3},
            'sucatas': {'wait_time': 15, 'max_retries': 5},  # ✅ MAIS TEMPO!
        }
        
        self.urls = [
            f"{self.base_url}/veiculos/lotes?sort=auction_date_init_asc",
            f"{self.base_url}/imoveis/lotes?sort=auction_date_init_asc",
            f"{self.base_url}/materiais/lotes?sort=auction_date_init_asc",
            f"{self.base_url}/sucatas/lotes?sort=auction_date_init_asc",
        ]
        
        self.stats = {
            'total_scraped': 0,
            'duplicates': 0,
            'with_bids': 0,
            'errors': 0,
        }
        
        self.section_counters = {}
    
    async def scrape(self) -> List[Dict]:
        """Scrape completo com interceptação passiva"""
        print("\n" + "="*60)
        print("🟣 SODRÉ SANTORO - VERSÃO CORRIGIDA")
        print("="*60)
        
        all_lots = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                locale='pt-BR'
            )
            
            page = await context.new_page()
            
            current_section = {'name': None, 'api_calls': 0, 'last_capture': 0}
            
            async def intercept_response(response):
                try:
                    if '/api/search-lots' in response.url and response.status == 200:
                        current_section['api_calls'] += 1
                        
                        data = await response.json()
                        per_page = data.get('perPage', 0)
                        
                        if per_page > 0:
                            results = data.get('results', [])
                            hits = data.get('hits', {}).get('hits', [])
                            
                            lots_captured = 0
                            
                            if results:
                                all_lots.extend(results)
                                lots_captured = len(results)
                            elif hits:
                                extracted = [hit.get('_source', hit) for hit in hits]
                                all_lots.extend(extracted)
                                lots_captured = len(extracted)
                            
                            if lots_captured > 0:
                                current_section['last_capture'] = time.time()
                                section = current_section['name']
                                if section not in self.section_counters:
                                    self.section_counters[section] = 0
                                self.section_counters[section] += lots_captured
                                
                                print(f"     📥 API call #{current_section['api_calls']}: +{lots_captured} lotes | Total: {self.section_counters[section]}")
                            else:
                                if self.debug:
                                    print(f"     ⚪ API call #{current_section['api_calls']}: 0 lotes (ainda carregando...)")
                except:
                    pass
            
            page.on('response', intercept_response)
            
            for url in self.urls:
                section_name = url.split('/')[3]
                current_section['name'] = section_name
                current_section['api_calls'] = 0
                current_section['last_capture'] = 0
                
                config = self.section_config.get(section_name, {'wait_time': 7, 'max_retries': 3})
                
                lots_before = len(all_lots)
                
                print(f"\n📦 {section_name.upper()}")
                print(f"  ⏱️ Tempo de espera: {config['wait_time']}s | Tentativas: {config['max_retries']}")
                
                try:
                    await page.goto(url, wait_until="networkidle", timeout=60000)
                    
                    # ✅ ESPERA ADAPTATIVA COM VERIFICAÇÃO
                    print(f"  ⏳ Aguardando carregamento...")
                    
                    for attempt in range(config['max_retries']):
                        await asyncio.sleep(config['wait_time'])
                        
                        lots_after = len(all_lots)
                        new_lots = lots_after - lots_before
                        
                        if new_lots > 0:
                            print(f"  ✅ Tentativa {attempt + 1}: {new_lots} lotes capturados")
                            break
                        else:
                            if attempt < config['max_retries'] - 1:
                                print(f"  🔄 Tentativa {attempt + 1}: Aguardando mais dados...")
                            else:
                                print(f"  ⚠️ Tentativa {attempt + 1}: Nenhum dado após {config['wait_time'] * config['max_retries']}s")
                    
                    # ✅ PAGINAÇÃO (se aplicável)
                    if len(all_lots) > lots_before:
                        consecutive_no_data = 0
                        
                        for page_num in range(2, 201):
                            try:
                                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                await asyncio.sleep(1)
                                
                                lots_before_click = len(all_lots)
                                
                                # Tenta clicar no botão de próxima página
                                selectors = [
                                    'button[title="Avançar"]:not([disabled])',
                                    'button[title="Avançar"]',
                                    'button:has-text("Avançar"):not([disabled])',
                                ]
                                
                                button_found = False
                                for selector in selectors:
                                    try:
                                        button = page.locator(selector).first
                                        count = await button.count()
                                        
                                        if count > 0:
                                            is_visible = await button.is_visible()
                                            is_enabled = await button.is_enabled()
                                            
                                            if is_visible and is_enabled:
                                                await button.click()
                                                button_found = True
                                                break
                                    except:
                                        continue
                                
                                if not button_found:
                                    if page_num > 2:  # Só mostra se paginou
                                        print(f"  ✅ {page_num-1} páginas")
                                    break
                                
                                await asyncio.sleep(5)
                                
                                lots_after_click = len(all_lots)
                                new_lots = lots_after_click - lots_before_click
                                
                                if new_lots == 0:
                                    consecutive_no_data += 1
                                    if consecutive_no_data >= 3:
                                        print(f"  ✅ {page_num} páginas (sem novos dados)")
                                        break
                                else:
                                    consecutive_no_data = 0
                            
                            except Exception as e:
                                if self.debug:
                                    print(f"  ⚠️ Erro na paginação: {type(e).__name__}")
                                break
                    
                    section_total = len(all_lots) - lots_before
                    print(f"  ✅ TOTAL DA SEÇÃO: {section_total} lotes")
                
                except Exception as e:
                    print(f"  ❌ Erro na seção: {e}")
            
            await browser.close()
        
        print(f"\n✅ {len(all_lots)} lotes capturados no total")
        
        # Processa lotes
        items = []
        categories = {}
        
        for lot in all_lots:
            try:
                item = self._normalize_lot(lot)
                if item:
                    items.append(item)
                    
                    # Contabiliza por categoria
                    cat = item.get('category', 'unknown')
                    if cat not in categories:
                        categories[cat] = {'total': 0, 'errors': 0}
                    categories[cat]['total'] += 1
                    
                    if item.get('has_bid'):
                        self.stats['with_bids'] += 1
            except Exception as e:
                self.stats['errors'] += 1
                if self.debug:
                    print(f"  ⚠️ Erro ao processar lote: {e}")
        
        self.stats['total_scraped'] = len(items)
        
        print(f"\n📊 Por Categoria:")
        for cat, stats in sorted(categories.items()):
            print(f"  • {cat}: {stats['total']}/{stats['total']} OK ({stats['errors']} erros)")
        
        return items
    
    def _normalize_lot(self, lot: Dict) -> Dict:
        """Normaliza um lote para o formato do banco"""
        try:
            # Campo obrigatório
            lot_id = lot.get('id')
            if not lot_id:
                return None
            
            external_id = f"sodre_{lot_id}"
            
            # Metadados flexíveis
            metadata = {}
            for key in ['lot_seller', 'lot_type_name', 'lot_subcategory']:
                val = lot.get(key)
                if val:
                    metadata[key] = val
            
            item = {
                'external_id': external_id,
                'source': self.source,
                'lot_id': str(lot_id),
                'title': self._safe_str(lot.get('title')) or self._safe_str(lot.get('lot_title')),
                'description': self._safe_str(lot.get('description')) or self._safe_str(lot.get('lot_description')),
                'category': self._safe_str(lot.get('category')),
                'subcategory': self._safe_str(lot.get('subcategory')),
                
                # URLs
                'url': f"{self.base_url}/lote/{lot_id}",
                'lot_url': self._safe_str(lot.get('lot_url')),
                'image_url': self._safe_str(lot.get('image_url')) or self._safe_str(lot.get('lot_image_url')),
                
                # Leilão
                'auction_id': self._safe_str(lot.get('auction_id')),
                'auction_date': self._parse_datetime(lot.get('auction_date')),
                'auction_type': self._safe_str(lot.get('auction_type')),
                
                # Valores
                'initial_bid': self._parse_numeric(lot.get('initial_bid')),
                'current_bid': self._parse_numeric(lot.get('current_bid')),
                'minimum_bid': self._parse_numeric(lot.get('minimum_bid')),
                
                # Lance
                'has_bid': bool(lot.get('bid_has_bid', False)),
                
                # Campos judiciais
                'lot_judicial_process': self._safe_str(lot.get('lot_judicial_process')),
                'lot_judicial_action': self._safe_str(lot.get('lot_judicial_action')),
                
                # Metadados
                'metadata': {k: v for k, v in metadata.items() if v is not None},
            }
            
            return item
        except:
            return None
    
    def _safe_str(self, value) -> str:
        if value is None:
            return None
        try:
            result = str(value).strip()
            return result if result else None
        except:
            return None
    
    def _parse_datetime(self, value) -> str:
        if not value:
            return None
        try:
            if isinstance(value, str):
                value = value.replace('Z', '+00:00')
                if 'T' in value:
                    return value
        except:
            pass
        return None
    
    def _parse_numeric(self, value):
        if value is None:
            return None
        try:
            return float(value)
        except:
            return None


async def main():
    print("\n" + "="*70)
    print("🚀 SODRÉ SANTORO - SCRAPER CORRIGIDO")
    print("="*70)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = time.time()
    supabase = None
    
    try:
        # Inicia Supabase se disponível
        if SupabaseClient:
            print("\n💓 Iniciando sistema de heartbeat...")
            supabase = SupabaseClient(
                service_name='sodre_scraper',
                service_type='scraper'
            )
            
            if supabase.test():
                supabase.heartbeat_start(metadata={
                    'scraper': 'sodre',
                    'version': 'fixed',
                    'sections': 4,
                })
        
        # Coleta dados
        print("\n🔥 FASE 1: COLETANDO DADOS")
        scraper = SodreScraperFixed(debug=True)
        items = await scraper.scrape()
        
        print(f"\n✅ Total coletado: {len(items)} itens")
        print(f"🔥 Itens com lances: {scraper.stats['with_bids']}")
        print(f"⚠️  Erros: {scraper.stats['errors']}")
        
        if not items:
            print("⚠️ Nenhum item coletado")
            if supabase:
                supabase.heartbeat_finish(status='warning', final_stats={
                    'items_collected': 0,
                })
            return
        
        # Salva JSON
        output_dir = Path(__file__).parent / 'data' / 'normalized'
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_file = output_dir / f'sodre_{timestamp}.json'
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        print(f"💾 JSON: {json_file}")
        
        # Insere no Supabase
        if supabase:
            print("\n📤 FASE 2: INSERINDO NO SUPABASE")
            print(f"\n  📤 sodre_items: {len(items)} itens")
            stats = supabase.upsert('sodre_items', items)
            
            print(f"    ✅ Inseridos: {stats['inserted']}")
            print(f"    🔄 Atualizados: {stats['updated']}")
            
            supabase.heartbeat_finish(status='inactive', final_stats={
                'items_collected': len(items),
                'items_inserted': stats['inserted'],
                'items_updated': stats['updated'],
                'items_with_bids': scraper.stats['with_bids'],
            })
    
    except Exception as e:
        print(f"⚠️ Erro crítico: {e}")
        if supabase:
            supabase.heartbeat_error(str(e)[:500])
    
    finally:
        elapsed = time.time() - start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        
        print("\n" + "="*70)
        print("📊 ESTATÍSTICAS FINAIS")
        print("="*70)
        print(f"🟣 Sodré Santoro:")
        print(f"  • Total coletado: {scraper.stats['total_scraped']}")
        print(f"  • Com lances: {scraper.stats['with_bids']}")
        print(f"  • Erros: {scraper.stats['errors']}")
        print(f"\n⏱️ Duração: {minutes}min {seconds}s")
        print(f"✅ Concluído: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    asyncio.run(main())