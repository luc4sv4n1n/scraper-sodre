#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SODRÉ SANTORO - SCRAPER COM PAGINAÇÃO ROBUSTA + HEARTBEAT
✅ Limite de 200 páginas (aumentado de 50)
✅ Múltiplos seletores para o botão
✅ Detecção de fim real de páginas
✅ Logs detalhados de debugging
✅ Sistema de heartbeat para monitoramento
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

from supabase_client import SupabaseClient


class SodreScraper:
    """Scraper Sodré com interceptação passiva da API + Heartbeat"""
    
    def __init__(self, debug=False):
        self.source = 'sodre'
        self.base_url = 'https://www.sodresantoro.com.br'
        self.leilao_base_url = 'https://leilao.sodresantoro.com.br'
        self.debug = debug
        
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
        
        self.failed_lots = []
        self.section_counters = {}
    
    async def scrape(self) -> List[Dict]:
        """Scrape completo com interceptação passiva"""
        print("\n" + "="*60)
        print("🟣 SODRÉ SANTORO - PAGINAÇÃO ROBUSTA v2")
        print("="*60)
        
        all_lots = []
        lots_by_section = {}
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                locale='pt-BR'
            )
            
            page = await context.new_page()
            
            current_section = {'name': None}
            
            async def intercept_response(response):
                try:
                    if '/api/search-lots' in response.url and response.status == 200:
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
                            
                            if lots_captured > 0 and current_section['name']:
                                section = current_section['name']
                                if section not in self.section_counters:
                                    self.section_counters[section] = 0
                                self.section_counters[section] += lots_captured
                                print(f"     📥 +{lots_captured} lotes | Total seção: {self.section_counters[section]}")
                except:
                    pass
            
            page.on('response', intercept_response)
            
            for url in self.urls:
                section_name = url.split('/')[3]
                current_section['name'] = section_name
                lots_before = len(all_lots)
                
                print(f"\n📦 {section_name.upper()}")
                
                try:
                    await page.goto(url, wait_until="networkidle", timeout=60000)
                    
                    print(f"  ⏳ Aguardando carregamento inicial (7s)...")
                    await asyncio.sleep(7)
                    
                    initial_capture = len(all_lots) - lots_before
                    if initial_capture > 0:
                        print(f"  ✅ Página 1: {initial_capture} lotes capturados")
                    
                    # ✅ PAGINAÇÃO MELHORADA - até 200 páginas
                    consecutive_no_data = 0  # Contador de tentativas sem novos dados
                    max_no_data = 3  # Máximo de tentativas sem dados antes de parar
                    
                    for page_num in range(2, 201):
                        try:
                            # Scroll para garantir carregamento
                            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            await asyncio.sleep(1)
                            
                            lots_before_click = len(all_lots)
                            
                            # ✅ MÚLTIPLOS SELETORES - tenta todos até encontrar
                            button_found = False
                            selectors = [
                                'button[title="Avançar"]:not([disabled])',
                                'button[title="Avançar"]',  # Tenta mesmo se tiver disabled (pode estar apenas hidden)
                                'button:has-text("Avançar"):not([disabled])',
                                'button.i-mdi\\:chevron-right:not([disabled])',  # Pelo ícone
                                '.pagination button:last-child:not([disabled])',
                            ]
                            
                            for selector in selectors:
                                try:
                                    button = page.locator(selector).first
                                    count = await button.count()
                                    
                                    if count > 0:
                                        # Verifica se está visível E clicável
                                        is_visible = await button.is_visible()
                                        is_enabled = await button.is_enabled()
                                        
                                        if is_visible and is_enabled:
                                            await button.click()
                                            button_found = True
                                            if self.debug:
                                                print(f"  🔍 Botão encontrado com: {selector}")
                                            break
                                        elif self.debug and count > 0:
                                            print(f"  ⚠️ Botão existe mas não está clicável (visible:{is_visible}, enabled:{is_enabled})")
                                except Exception as e:
                                    if self.debug:
                                        print(f"  ⚠️ Erro ao tentar {selector}: {type(e).__name__}")
                                    continue
                            
                            if not button_found:
                                if self.debug:
                                    # Tenta ver o que existe na página
                                    all_buttons = await page.locator('button').count()
                                    print(f"  ℹ️ Total de botões na página: {all_buttons}")
                                    
                                print(f"  ✅ {page_num-1} páginas - botão não encontrado")
                                break
                            
                            print(f"  ➡️ Página {page_num}...")
                            
                            # Espera após o click
                            await asyncio.sleep(5)
                            
                            # ✅ VERIFICA SE CAPTUROU NOVOS DADOS
                            lots_after_click = len(all_lots)
                            new_lots = lots_after_click - lots_before_click
                            
                            if new_lots == 0:
                                consecutive_no_data += 1
                                if self.debug:
                                    print(f"    ⚠️ Nenhum lote novo ({consecutive_no_data}/{max_no_data})")
                                
                                if consecutive_no_data >= max_no_data:
                                    print(f"  ℹ️ Parando: {consecutive_no_data} tentativas sem novos dados")
                                    break
                            else:
                                consecutive_no_data = 0  # Reset se capturou dados
                                
                        except Exception as e:
                            if self.debug:
                                print(f"  ⚠️ Erro na página {page_num}: {type(e).__name__} - {str(e)}")
                            break
                
                except Exception as e:
                    print(f"  ⚠️ Erro na seção: {e}")
                
                lots_after = len(all_lots)
                section_lots = lots_after - lots_before
                lots_by_section[section_name] = section_lots
                
                if section_lots == 0:
                    print(f"  ⚠️  Nenhum lote capturado nesta seção")
                else:
                    print(f"  ✅ TOTAL DA SEÇÃO: {section_lots} lotes")
            
            await browser.close()
        
        print(f"\n✅ {len(all_lots)} lotes capturados no total")
        print(f"\n📊 Por seção:")
        for section, count in lots_by_section.items():
            print(f"  • {section}: {count} lotes")
        
        items = self._process_lots(all_lots)
        
        self.stats['total_scraped'] = len(items)
        return items
    
    def _process_lots(self, lots: List[Dict]) -> List[Dict]:
        """Processa lotes da API"""
        print("\n📋 Processando lotes...")
        
        items = []
        seen_ids = set()
        error_reasons = {}
        category_stats = {}
        
        for lot in lots:
            try:
                category = lot.get('segment_label', 'desconhecido')
                if category not in category_stats:
                    category_stats[category] = {'total': 0, 'ok': 0, 'error': 0}
                category_stats[category]['total'] += 1
                
                item = self._extract_lot_data(lot)
                
                if not item:
                    reason = 'extract_failed'
                    error_reasons[reason] = error_reasons.get(reason, 0) + 1
                    category_stats[category]['error'] += 1
                    self.stats['errors'] += 1
                    
                    if self.debug:
                        self.failed_lots.append({'lot': lot, 'reason': 'extract_failed'})
                    continue
                
                if item['external_id'] in seen_ids:
                    self.stats['duplicates'] += 1
                    continue
                
                items.append(item)
                seen_ids.add(item['external_id'])
                category_stats[category]['ok'] += 1
                
                if item.get('has_bid'):
                    self.stats['with_bids'] += 1
                
            except Exception as e:
                reason = str(type(e).__name__)
                error_reasons[reason] = error_reasons.get(reason, 0) + 1
                category_stats.get(category, {})['error'] = category_stats.get(category, {}).get('error', 0) + 1
                self.stats['errors'] += 1
                
                if self.debug:
                    self.failed_lots.append({'lot': lot, 'reason': f'{reason}: {str(e)}'})
        
        print(f"\n📊 Por Categoria:")
        for cat, stats in sorted(category_stats.items()):
            print(f"  • {cat}: {stats['ok']}/{stats['total']} OK ({stats['error']} erros)")
        
        if error_reasons:
            print(f"\n⚠️  Motivos dos erros:")
            for reason, count in sorted(error_reasons.items(), key=lambda x: x[1], reverse=True):
                print(f"  • {reason}: {count}")
        
        print(f"\n✅ {len(items)} itens válidos processados")
        return items
    
    def _extract_lot_data(self, lot: Dict) -> dict:
        """Extrai TODOS os dados do lote para sodre_items"""
        try:
            auction_id = lot.get('auction_id')
            lot_id = lot.get('lot_id')
            
            if not auction_id or not lot_id:
                return None
            
            try:
                lot_id = int(lot_id)
            except:
                index_id = lot.get('index_id') or lot.get('id')
                if index_id:
                    try:
                        lot_id = int(index_id)
                    except:
                        return None
                else:
                    return None
            
            external_id = f"sodre_{lot_id}"
            
            title = (
                self._safe_str(lot.get('lot_title')) or
                self._safe_str(lot.get('lot_type_name')) or
                self._safe_str(lot.get('title'))
            )
            
            if not title or len(title) < 3:
                return None
            
            link = f"{self.leilao_base_url}/leilao/{auction_id}/lote/{lot_id}/"
            
            # Datas
            auction_date_init = self._parse_datetime(lot.get('auction_date_init'))
            auction_date_2 = self._parse_datetime(lot.get('auction_date_2'))
            auction_date_end = self._parse_datetime(lot.get('auction_date_end'))
            
            # Localização
            lot_location = self._safe_str(lot.get('lot_location'))
            
            # Imagem
            image_url = None
            lot_pictures = lot.get('lot_pictures')
            if lot_pictures:
                if isinstance(lot_pictures, list) and len(lot_pictures) > 0:
                    image_url = lot_pictures[0]
                elif isinstance(lot_pictures, str):
                    image_url = lot_pictures
            
            # Optionals
            lot_optionals = lot.get('lot_optionals')
            if lot_optionals:
                if isinstance(lot_optionals, list):
                    lot_optionals = [str(opt) for opt in lot_optionals if opt]
                elif isinstance(lot_optionals, str):
                    lot_optionals = [lot_optionals]
                else:
                    lot_optionals = None
            else:
                lot_optionals = None
            
            # Metadata base
            metadata = {
                'segment_base': lot.get('segment_base'),
                'lot_pictures': lot_pictures if lot_pictures else None,
                'search_terms': lot.get('search_terms'),
            }
            
            item = {
                'external_id': external_id,
                'lot_id': lot_id,
                'lot_number': self._safe_str(lot.get('lot_number')),
                'lot_inspection_number': self._safe_str(lot.get('lot_inspection_number')),
                'lot_inspection_id': self._parse_int(lot.get('lot_inspection_id')),
                'auction_id': int(auction_id),
                'category': self._safe_str(lot.get('lot_category')),
                'segment_id': self._safe_str(lot.get('segment_id')),
                'segment_label': self._safe_str(lot.get('segment_label')),
                'segment_slug': self._safe_str(lot.get('segment_slug')),
                'lot_category': self._safe_str(lot.get('lot_category')),
                'title': title,
                'description': self._safe_str(lot.get('lot_description')),
                'lot_location': lot_location,
                'auction_name': self._safe_str(lot.get('auction_name')),
                'auction_status': self._safe_str(lot.get('auction_status')),
                'auction_date_init': auction_date_init,
                'auction_date_2': auction_date_2,
                'auction_date_end': auction_date_end,
                'auctioneer_name': self._safe_str(lot.get('auctioneer_name')),
                'client_id': self._parse_int(lot.get('client_id')),
                'client_name': self._safe_str(lot.get('client_name')),
                'bid_initial': self._parse_numeric(lot.get('bid_initial')),
                'bid_actual': self._parse_numeric(lot.get('bid_actual')),
                'bid_has_bid': bool(lot.get('bid_has_bid', False)),
                'bid_user_nickname': self._safe_str(lot.get('bid_user_nickname')),
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
                'lot_optionals': lot_optionals,
                'lot_tags': self._safe_str(lot.get('lot_tags')),
                'image_url': image_url,
                'lot_status': self._safe_str(lot.get('lot_status')),
                'lot_status_id': self._parse_int(lot.get('lot_status_id')),
                'lot_is_judicial': bool(lot.get('lot_is_judicial', False)),
                'lot_is_scrap': bool(lot.get('lot_is_scrap', False)),
                'lot_financeable': bool(lot.get('lot_status_financeable', False)),
                'is_highlight': bool(lot.get('is_highlight', False)),
                'lot_test': bool(lot.get('lot_test', False)),
                'lot_visits': self._parse_int(lot.get('lot_visits')) or 0,
                'link': link,
                'source': 'sodre',
                'is_active': True,
                'has_bid': bool(lot.get('bid_has_bid', False)),
                
                # Campos judiciais
                'lot_judicial_process': self._safe_str(lot.get('lot_judicial_process')),
                'lot_judicial_action': self._safe_str(lot.get('lot_judicial_action')),
                'lot_judicial_executor': self._safe_str(lot.get('lot_judicial_executor')),
                'lot_judicial_executed': self._safe_str(lot.get('lot_judicial_executed')),
                'lot_judicial_judge': self._safe_str(lot.get('lot_judicial_judge')),
                'tj_praca_value': self._parse_numeric(lot.get('tj_praca_value')),
                'tj_praca_discount': self._parse_numeric(lot.get('tj_praca_discount')),
                'lot_neighborhood': self._safe_str(lot.get('lot_neighborhood')),
                'lot_street': self._safe_str(lot.get('lot_street')),
                'lot_dormitories': self._parse_int(lot.get('lot_dormitories')),
                'lot_useful_area': self._parse_numeric(lot.get('lot_useful_area')),
                'lot_total_area': self._parse_numeric(lot.get('lot_total_area')),
                'lot_suites': self._parse_int(lot.get('lot_suites')),
                
                # Campos materiais
                'lot_subcategory': self._safe_str(lot.get('lot_subcategory')),
                'lot_type_name': self._safe_str(lot.get('lot_type_name')),
                
                'metadata': {k: v for k, v in metadata.items() if v is not None},
            }
            
            return item
            
        except Exception as e:
            return None
    
    def _safe_str(self, value) -> str:
        """Converte para string de forma segura"""
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
                else:
                    dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                    return dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')
            return None
        except:
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
    print("🚀 SODRÉ SANTORO - SCRAPER MELHORADO + HEARTBEAT")
    print("="*70)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = time.time()
    supabase = None
    
    try:
        # ✅ INICIA SUPABASE COM HEARTBEAT
        print("\n💓 Iniciando sistema de heartbeat...")
        supabase = SupabaseClient(
            service_name='sodre_scraper',
            service_type='scraper'
        )
        
        # ✅ TESTA CONEXÃO
        if not supabase.test():
            print("⚠️ Erro no Supabase - continuando sem heartbeat")
        else:
            # ✅ INICIA HEARTBEAT
            supabase.heartbeat_start(metadata={
                'scraper': 'sodre',
                'sections': 4,
                'max_pages_per_section': 200
            })
        
        # ✅ COLETA DADOS
        print("\n🔥 FASE 1: COLETANDO DADOS")
        scraper = SodreScraper(debug=True)
        items = await scraper.scrape()
        
        print(f"\n✅ Total coletado: {len(items)} itens")
        print(f"🔥 Itens com lances: {scraper.stats['with_bids']}")
        print(f"🔄 Duplicatas: {scraper.stats['duplicates']}")
        print(f"⚠️  Erros: {scraper.stats['errors']}")
        
        if not items:
            print("⚠️ Nenhum item coletado")
            
            # ✅ FINALIZA HEARTBEAT COM ERRO
            if supabase:
                supabase.heartbeat_finish(status='warning', final_stats={
                    'items_collected': 0,
                    'reason': 'no_items_collected'
                })
            return
        
        # ✅ SALVA JSON
        output_dir = Path(__file__).parent / 'data' / 'normalized'
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_file = output_dir / f'sodre_{timestamp}.json'
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        print(f"💾 JSON: {json_file}")
        
        if scraper.failed_lots:
            failed_file = output_dir / f'sodre_failed_{timestamp}.json'
            with open(failed_file, 'w', encoding='utf-8') as f:
                json.dump(scraper.failed_lots[:10], f, ensure_ascii=False, indent=2)
            print(f"🛠 Debug (primeiros 10 erros): {failed_file}")
        
        # ✅ INSERE NO SUPABASE
        print("\n📤 FASE 2: INSERINDO NO SUPABASE")
        
        if supabase:
            print(f"\n  📤 sodre_items: {len(items)} itens")
            stats = supabase.upsert('sodre_items', items)
            
            print(f"    ✅ Inseridos: {stats['inserted']}")
            print(f"    🔄 Atualizados: {stats['updated']}")
            if stats['errors'] > 0:
                print(f"    ⚠️ Erros: {stats['errors']}")
            
            # ✅ FINALIZA HEARTBEAT COM SUCESSO
            supabase.heartbeat_finish(status='inactive', final_stats={
                'items_collected': len(items),
                'items_inserted': stats['inserted'],
                'items_updated': stats['updated'],
                'items_with_bids': scraper.stats['with_bids'],
                'duplicates': scraper.stats['duplicates'],
                'errors': scraper.stats['errors'],
            })
    
    except Exception as e:
        print(f"⚠️ Erro crítico: {e}")
        
        # ✅ REGISTRA ERRO NO HEARTBEAT
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
        print(f"  • Duplicatas: {scraper.stats['duplicates']}")
        print(f"  • Erros: {scraper.stats['errors']}")
        print(f"\n⏱️ Duração: {minutes}min {seconds}s")
        print(f"✅ Concluído: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)


if __name__ == "__main__":
    asyncio.run(main())