#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SODRÉ SANTORO - SCRAPER CORRIGIDO
✅ Paginação robusta - não para prematuramente
✅ Espera adaptativa por seção
✅ Deduplicação na coleta
✅ Mapeamento completo dos campos
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
    from supabase_client_fixed import SupabaseClient
except:
    try:
        from supabase_client import SupabaseClient
    except:
        SupabaseClient = None


class SodreScraperFixed:
    """Scraper Sodré - Versão Corrigida"""
    
    def __init__(self, debug=False):
        self.source = 'sodre'
        self.base_url = 'https://www.sodresantoro.com.br'
        self.debug = debug
        
        # ✅ Configuração otimizada por seção
        self.section_config = {
            'veiculos': {'wait_time': 7, 'max_retries': 3, 'max_pages': 200},
            'imoveis': {'wait_time': 7, 'max_retries': 3, 'max_pages': 50},
            'materiais': {'wait_time': 7, 'max_retries': 3, 'max_pages': 200},
            'sucatas': {'wait_time': 12, 'max_retries': 4, 'max_pages': 200},
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
        seen_lot_ids = set()  # ✅ Deduplicação na coleta
        
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
                            new_lots = 0
                            
                            # Extrai lotes da resposta
                            lots_to_add = []
                            if results:
                                lots_to_add = results
                            elif hits:
                                lots_to_add = [hit.get('_source', hit) for hit in hits]
                            
                            # ✅ Deduplica durante a coleta
                            for lot in lots_to_add:
                                lot_id = lot.get('id') or lot.get('lot_id')
                                if lot_id and lot_id not in seen_lot_ids:
                                    seen_lot_ids.add(lot_id)
                                    all_lots.append(lot)
                                    new_lots += 1
                            
                            if new_lots > 0:
                                current_section['last_capture'] = time.time()
                                section = current_section['name']
                                if section not in self.section_counters:
                                    self.section_counters[section] = 0
                                self.section_counters[section] += new_lots
                                
                                print(f"     📥 API call #{current_section['api_calls']}: +{new_lots} lotes únicos | Total: {self.section_counters[section]}")
                            else:
                                if self.debug:
                                    total = len(lots_to_add)
                                    print(f"     ⚪ API call #{current_section['api_calls']}: 0 novos ({total} duplicatas)")
                except:
                    pass
            
            page.on('response', intercept_response)
            
            for url in self.urls:
                section_name = url.split('/')[3]
                current_section['name'] = section_name
                current_section['api_calls'] = 0
                current_section['last_capture'] = 0
                
                config = self.section_config.get(section_name, {'wait_time': 7, 'max_retries': 3, 'max_pages': 200})
                
                lots_before = len(all_lots)
                
                print(f"\n📦 {section_name.upper()}")
                print(f"  ⏱️ Tempo de espera: {config['wait_time']}s | Máx páginas: {config['max_pages']}")
                
                try:
                    await page.goto(url, wait_until="networkidle", timeout=60000)
                    
                    print(f"  ⏳ Aguardando carregamento inicial...")
                    
                    # ✅ Espera inicial adaptativa
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
                                print(f"  ⚠️ Tentativa {attempt + 1}: Nenhum dado capturado")
                    
                    # ✅ PAGINAÇÃO ROBUSTA
                    if len(all_lots) > lots_before:
                        # Contador de tentativas sem sucesso de CLICK (não de dados)
                        failed_clicks = 0
                        max_failed_clicks = 5
                        
                        for page_num in range(2, config['max_pages'] + 1):
                            try:
                                # Scroll
                                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                await asyncio.sleep(1)
                                
                                # Tenta encontrar e clicar no botão
                                selectors = [
                                    'button[title="Avançar"]:not([disabled])',
                                    'button[title="Avançar"]',
                                    'button:has-text("Avançar"):not([disabled])',
                                    'button.i-mdi\\:chevron-right:not([disabled])',
                                    '.pagination button:last-child:not([disabled])',
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
                                                failed_clicks = 0  # Reset contador
                                                break
                                    except:
                                        continue
                                
                                if not button_found:
                                    failed_clicks += 1
                                    if self.debug:
                                        print(f"    ⚠️ Botão não encontrado (tentativa {failed_clicks}/{max_failed_clicks})")
                                    
                                    if failed_clicks >= max_failed_clicks:
                                        print(f"  ✅ {page_num-1} páginas - fim detectado")
                                        break
                                    
                                    # Espera um pouco antes de tentar novamente
                                    await asyncio.sleep(2)
                                    continue
                                
                                print(f"  ➡️ Página {page_num}...")
                                
                                # ✅ Espera adaptativa após click
                                # Não precisa verificar novos dados imediatamente,
                                # a interceptação vai capturar quando chegarem
                                await asyncio.sleep(5)
                                
                            except Exception as e:
                                if self.debug:
                                    print(f"  ⚠️ Erro na página {page_num}: {type(e).__name__}")
                                break
                    
                    section_total = len(all_lots) - lots_before
                    print(f"  ✅ TOTAL DA SEÇÃO: {section_total} lotes únicos")
                
                except Exception as e:
                    print(f"  ❌ Erro: {e}")
            
            await browser.close()
        
        print(f"\n✅ {len(all_lots)} lotes únicos capturados no total")
        
        # Processa lotes
        items = []
        categories = {}
        
        for lot in all_lots:
            try:
                item = self._normalize_lot(lot)
                if item:
                    items.append(item)
                    
                    cat = item.get('category', 'unknown')
                    if cat not in categories:
                        categories[cat] = {'total': 0, 'errors': 0}
                    categories[cat]['total'] += 1
                    
                    if item.get('has_bid'):
                        self.stats['with_bids'] += 1
            except Exception as e:
                self.stats['errors'] += 1
        
        self.stats['total_scraped'] = len(items)
        
        print(f"\n📊 Por Categoria:")
        for cat, stats in sorted(categories.items()):
            print(f"  • {cat}: {stats['total']}/{stats['total']} OK")
        
        return items
    
    def _normalize_lot(self, lot: Dict) -> Dict:
        """
        Normaliza lote para o schema exato de sodre_items
        ✅ Todos os campos mapeados corretamente
        """
        try:
            lot_id = lot.get('id') or lot.get('lot_id')
            if not lot_id:
                return None
            
            try:
                lot_id = int(lot_id)
            except:
                return None
            
            external_id = f"sodre_{lot_id}"
            
            # ✅ MAPEAMENTO COMPLETO CONFORME SCHEMA
            item = {
                # IDs e identificadores
                'external_id': external_id,
                'lot_id': lot_id,
                'lot_number': self._safe_str(lot.get('lot_number')),
                'lot_inspection_number': self._safe_str(lot.get('lot_inspection_number')),
                'lot_inspection_id': self._parse_int(lot.get('lot_inspection_id')),
                'auction_id': self._parse_int(lot.get('auction_id')),
                
                # Categorias e segmentos
                'category': self._safe_str(lot.get('category') or lot.get('lot_category')),
                'segment_id': self._safe_str(lot.get('segment_id')),
                'segment_label': self._safe_str(lot.get('segment_label')),
                'segment_slug': self._safe_str(lot.get('segment_slug')),
                'lot_category': self._safe_str(lot.get('lot_category')),
                
                # Textos principais
                'title': (
                    self._safe_str(lot.get('title')) or 
                    self._safe_str(lot.get('lot_title')) or 
                    self._safe_str(lot.get('lot_type_name')) or 
                    'Sem título'
                ),
                'description': self._safe_str(lot.get('description') or lot.get('lot_description')),
                
                # Localização
                'lot_location': self._safe_str(lot.get('lot_location')),
                'city': self._safe_str(lot.get('city')),
                'state': self._safe_str(lot.get('state')),
                
                # Leilão
                'auction_name': self._safe_str(lot.get('auction_name')),
                'auction_status': self._safe_str(lot.get('auction_status')),
                'auction_date_init': self._parse_datetime(lot.get('auction_date_init') or lot.get('auction_date')),
                'auction_date_2': self._parse_datetime(lot.get('auction_date_2')),
                'auction_date_end': self._parse_datetime(lot.get('auction_date_end')),
                
                # Leiloeiro e cliente
                'auctioneer_name': self._safe_str(lot.get('auctioneer_name')),
                'client_id': self._parse_int(lot.get('client_id')),
                'client_name': self._safe_str(lot.get('client_name')),
                
                # Lances
                'bid_initial': self._parse_numeric(lot.get('bid_initial') or lot.get('initial_bid')),
                'bid_actual': self._parse_numeric(lot.get('bid_actual') or lot.get('current_bid')),
                'bid_has_bid': bool(lot.get('bid_has_bid', False)),
                'bid_user_nickname': self._safe_str(lot.get('bid_user_nickname')),
                
                # Veículos
                'lot_brand': self._safe_str(lot.get('lot_brand')),
                'lot_model': self._safe_str(lot.get('lot_model')),
                'lot_year_manufacture': self._parse_int(lot.get('lot_year_manufacture')),
                'lot_year_model': self._parse_int(lot.get('lot_year_model')),
                'lot_plate': self._safe_str(lot.get('lot_plate')),
                'lot_color': self._safe_str(lot.get('lot_color')),
                'lot_km': self._parse_int(lot.get('lot_km')),
                'lot_fuel': self._safe_str(lot.get('lot_fuel')),
                'lot_transmission': self._safe_str(lot.get('lot_transmission')),
                'lot_sinister': self._safe_str(lot.get('lot_sinister')),
                'lot_origin': self._safe_str(lot.get('lot_origin')),
                'lot_optionals': self._parse_optionals(lot.get('lot_optionals')),
                'lot_tags': self._safe_str(lot.get('lot_tags')),
                
                # Imagem e link
                'image_url': self._parse_image(lot.get('image_url') or lot.get('lot_image_url') or lot.get('lot_pictures')),
                'link': f"{self.base_url}/lote/{lot_id}",
                
                # Status e flags
                'lot_status': self._safe_str(lot.get('lot_status')),
                'lot_status_id': self._parse_int(lot.get('lot_status_id')),
                'lot_is_judicial': bool(lot.get('lot_is_judicial', False)),
                'lot_is_scrap': bool(lot.get('lot_is_scrap', False)),
                'lot_financeable': bool(lot.get('lot_financeable') or lot.get('lot_status_financeable', False)),
                'is_highlight': bool(lot.get('is_highlight', False)),
                'lot_test': bool(lot.get('lot_test', False)),
                'lot_visits': self._parse_int(lot.get('lot_visits')) or 0,
                
                # Source e controle
                'source': self.source,
                'is_active': True,
                'has_bid': bool(lot.get('bid_has_bid', False)),
                
                # Campos judiciais (imóveis)
                'lot_judicial_process': self._safe_str(lot.get('lot_judicial_process')),
                'lot_judicial_action': self._safe_str(lot.get('lot_judicial_action')),
                'lot_judicial_executor': self._safe_str(lot.get('lot_judicial_executor')),
                'lot_judicial_executed': self._safe_str(lot.get('lot_judicial_executed')),
                'lot_judicial_judge': self._safe_str(lot.get('lot_judicial_judge')),
                'tj_praca_value': self._parse_numeric(lot.get('tj_praca_value')),
                'tj_praca_discount': self._parse_numeric(lot.get('tj_praca_discount')),
                
                # Imóveis - endereço
                'lot_neighborhood': self._safe_str(lot.get('lot_neighborhood')),
                'lot_street': self._safe_str(lot.get('lot_street')),
                
                # Imóveis - características
                'lot_dormitories': self._parse_int(lot.get('lot_dormitories')),
                'lot_useful_area': self._parse_numeric(lot.get('lot_useful_area')),
                'lot_total_area': self._parse_numeric(lot.get('lot_total_area')),
                'lot_suites': self._parse_int(lot.get('lot_suites')),
                
                # Materiais
                'lot_subcategory': self._safe_str(lot.get('lot_subcategory') or lot.get('subcategory')),
                'lot_type_name': self._safe_str(lot.get('lot_type_name')),
                
                # Metadata
                'metadata': self._build_metadata(lot),
            }
            
            # Remove None values
            return {k: v for k, v in item.items() if v is not None}
        
        except Exception as e:
            if self.debug:
                print(f"  ⚠️ Erro ao normalizar lote {lot.get('id')}: {e}")
            return None
    
    def _build_metadata(self, lot: Dict) -> Dict:
        """Constrói metadata com campos extras"""
        metadata = {}
        
        # Campos extras
        extra_fields = [
            'segment_base', 'search_terms',
        ]
        
        for field in extra_fields:
            val = lot.get(field)
            if val:
                metadata[field] = val
        
        return metadata if metadata else {}
    
    def _parse_optionals(self, value):
        """Parse lot_optionals para array"""
        if not value:
            return None
        if isinstance(value, list):
            return [str(opt) for opt in value if opt]
        if isinstance(value, str):
            return [value]
        return None
    
    def _parse_image(self, value):
        """Parse image_url ou lot_pictures"""
        if not value:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, list) and len(value) > 0:
            return value[0]
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
                try:
                    dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                    return dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')
                except:
                    pass
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
    
    def _parse_int(self, value):
        if value is None:
            return None
        try:
            return int(value)
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
        if SupabaseClient:
            print("\n💓 Iniciando sistema de heartbeat...")
            supabase = SupabaseClient(
                service_name='sodre_scraper',
                service_type='scraper'
            )
            
            if supabase.test():
                supabase.heartbeat_start(metadata={
                    'scraper': 'sodre',
                    'version': 'corrigida',
                })
        
        print("\n🔥 FASE 1: COLETANDO DADOS")
        scraper = SodreScraperFixed(debug=False)
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
            
            print(f"    ✅ Inseridos/Atualizados: {stats['inserted']}")
            if stats.get('duplicates_removed', 0) > 0:
                print(f"    🔄 Duplicatas removidas: {stats['duplicates_removed']}")
            if stats['errors'] > 0:
                print(f"    ⚠️ Erros: {stats['errors']}")
            
            supabase.heartbeat_finish(status='inactive', final_stats={
                'items_collected': len(items),
                'items_inserted': stats['inserted'],
                'items_with_bids': scraper.stats['with_bids'],
                'duplicates_removed': stats.get('duplicates_removed', 0),
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