#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SODRÉ SANTORO - SCRAPER COMPLETO
✅ Mapeamento TOTAL para tabela sodre_items
✅ Suporta: veículos, imóveis judiciais, materiais, sucatas
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
    """Scraper Sodré com interceptação passiva da API"""
    
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
    
    async def scrape(self) -> List[Dict]:
        """Scrape completo com interceptação passiva"""
        print("\n" + "="*60)
        print("🟣 SODRÉ SANTORO - INTERCEPTAÇÃO PASSIVA")
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
            
            async def intercept_response(response):
                try:
                    if '/api/search-lots' in response.url and response.status == 200:
                        data = await response.json()
                        per_page = data.get('perPage', 0)
                        
                        if per_page > 0:
                            results = data.get('results', [])
                            hits = data.get('hits', {}).get('hits', [])
                            
                            if results:
                                all_lots.extend(results)
                            elif hits:
                                extracted = [hit.get('_source', hit) for hit in hits]
                                all_lots.extend(extracted)
                except:
                    pass
            
            page.on('response', intercept_response)
            
            for url in self.urls:
                section_name = url.split('/')[3]
                lots_before = len(all_lots)
                
                print(f"\n📦 {section_name.upper()}")
                
                try:
                    await page.goto(url, wait_until="networkidle", timeout=60000)
                    await asyncio.sleep(3)
                    
                    for page_num in range(2, 51):
                        try:
                            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            await asyncio.sleep(2)
                            
                            button = page.locator('button[title="Avançar"]:not([disabled])').first
                            if await button.count() > 0:
                                await button.click()
                                print(f"  ➡️  Página {page_num}...")
                                await asyncio.sleep(4)
                            else:
                                print(f"  ✅ {page_num-1} páginas")
                                break
                        except:
                            break
                
                except Exception as e:
                    print(f"  ⚠️ Erro: {e}")
                
                lots_after = len(all_lots)
                section_lots = lots_after - lots_before
                lots_by_section[section_name] = section_lots
                
                if section_lots == 0:
                    print(f"  ⚠️  Nenhum lote capturado")
            
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
            
            title = lot.get('lot_title', '').strip()
            if not title or len(title) < 3:
                return None
            
            link = f"{self.leilao_base_url}/leilao/{auction_id}/lote/{lot_id}/"
            
            # Datas
            auction_date_init = self._parse_datetime(lot.get('auction_date_init'))
            auction_date_2 = self._parse_datetime(lot.get('auction_date_2'))
            auction_date_end = self._parse_datetime(lot.get('auction_date_end'))
            
            # Localização
            lot_location = lot.get('lot_location', '').strip() or None
            
            # Imagem
            image_url = None
            lot_pictures = lot.get('lot_pictures', [])
            if lot_pictures and isinstance(lot_pictures, list) and len(lot_pictures) > 0:
                image_url = lot_pictures[0]
            
            # Optionals
            lot_optionals = lot.get('lot_optionals')
            if lot_optionals and isinstance(lot_optionals, list):
                lot_optionals = [str(opt) for opt in lot_optionals if opt]
            else:
                lot_optionals = None
            
            # Metadata base
            metadata = {
                'segment_base': lot.get('segment_base'),
                'lot_pictures': lot_pictures if lot_pictures else None,
                'search_terms': lot.get('search_terms'),
            }
            
            # ✅ ITEM BASE COM TODOS OS CAMPOS
            item = {
                'external_id': external_id,
                'lot_id': lot_id,
                'lot_number': lot.get('lot_number', '').strip() or None,
                'lot_inspection_number': lot.get('lot_inspection_number', '').strip() or None,
                'lot_inspection_id': self._parse_int(lot.get('lot_inspection_id')),
                'auction_id': int(auction_id),
                'category': lot.get('lot_category', '').strip() or None,
                'segment_id': lot.get('segment_id', '').strip() or None,
                'segment_label': lot.get('segment_label', '').strip() or None,
                'segment_slug': lot.get('segment_slug', '').strip() or None,
                'lot_category': lot.get('lot_category', '').strip() or None,
                'title': title,
                'description': lot.get('lot_description', '').strip() or None,
                'lot_location': lot_location,
                'auction_name': lot.get('auction_name', '').strip() or None,
                'auction_status': lot.get('auction_status', '').strip() or None,
                'auction_date_init': auction_date_init,
                'auction_date_2': auction_date_2,
                'auction_date_end': auction_date_end,
                'auctioneer_name': lot.get('auctioneer_name', '').strip() or None,
                'client_id': self._parse_int(lot.get('client_id')),
                'client_name': lot.get('client_name', '').strip() or None,
                'bid_initial': self._parse_numeric(lot.get('bid_initial')),
                'bid_actual': self._parse_numeric(lot.get('bid_actual')),
                'bid_has_bid': bool(lot.get('bid_has_bid', False)),
                'bid_user_nickname': lot.get('bid_user_nickname', '').strip() or None,
                'lot_brand': lot.get('lot_brand', '').strip() or None,
                'lot_model': lot.get('lot_model', '').strip() or None,
                'lot_year_manufacture': self._parse_int(lot.get('lot_year_manufacture')),
                'lot_year_model': self._parse_int(lot.get('lot_year_model')),
                'lot_plate': lot.get('lot_plate', '').strip() or None,
                'lot_color': lot.get('lot_color', '').strip() or None,
                'lot_km': self._parse_int(lot.get('lot_km')),
                'lot_fuel': lot.get('lot_fuel', '').strip() or None,
                'lot_transmission': lot.get('lot_transmission', '').strip() or None,
                'lot_sinister': lot.get('lot_sinister', '').strip() or None,
                'lot_origin': lot.get('lot_origin', '').strip() or None,
                'lot_optionals': lot_optionals,
                'lot_tags': lot.get('lot_tags', '').strip() or None,
                'image_url': image_url,
                'lot_status': lot.get('lot_status', '').strip() or None,
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
                
                # ✅ NOVOS CAMPOS - JUDICIAIS
                'lot_judicial_process': lot.get('lot_judicial_process', '').strip() or None,
                'lot_judicial_action': lot.get('lot_judicial_action', '').strip() or None,
                'lot_judicial_executor': lot.get('lot_judicial_executor', '').strip() or None,
                'lot_judicial_executed': lot.get('lot_judicial_executed', '').strip() or None,
                'lot_judicial_judge': lot.get('lot_judicial_judge', '').strip() or None,
                'tj_praca_value': self._parse_numeric(lot.get('tj_praca_value')),
                'tj_praca_discount': self._parse_numeric(lot.get('tj_praca_discount')),
                'lot_neighborhood': lot.get('lot_neighborhood', '').strip() or None,
                'lot_street': lot.get('lot_street', '').strip() or None,
                'lot_dormitories': self._parse_int(lot.get('lot_dormitories')),
                'lot_useful_area': self._parse_numeric(lot.get('lot_useful_area')),
                'lot_total_area': self._parse_numeric(lot.get('lot_total_area')),
                'lot_suites': self._parse_int(lot.get('lot_suites')),
                
                # ✅ NOVOS CAMPOS - MATERIAIS
                'lot_subcategory': lot.get('lot_subcategory', '').strip() or None,
                'lot_type_name': lot.get('lot_type_name', '').strip() or None,
                
                'metadata': {k: v for k, v in metadata.items() if v is not None},
            }
            
            return item
            
        except Exception as e:
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
    print("🚀 SODRÉ SANTORO - SCRAPER COMPLETO")
    print("="*70)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = time.time()
    
    print("\n🔥 FASE 1: COLETANDO DADOS")
    scraper = SodreScraper(debug=True)
    items = await scraper.scrape()
    
    print(f"\n✅ Total coletado: {len(items)} itens")
    print(f"🔥 Itens com lances: {scraper.stats['with_bids']}")
    print(f"🔄 Duplicatas: {scraper.stats['duplicates']}")
    print(f"⚠️  Erros: {scraper.stats['errors']}")
    
    if not items:
        print("⚠️ Nenhum item coletado")
        return
    
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
        print(f"🐛 Debug (primeiros 10 erros): {failed_file}")
    
    print("\n📤 FASE 2: INSERINDO NO SUPABASE")
    
    try:
        supabase = SupabaseClient()
        
        if not supabase.test():
            print("⚠️ Erro no Supabase")
        else:
            print(f"\n  📤 sodre_items: {len(items)} itens")
            stats = supabase.upsert('sodre_items', items)
            
            print(f"    ✅ Inseridos: {stats['inserted']}")
            print(f"    🔄 Atualizados: {stats['updated']}")
            if stats['errors'] > 0:
                print(f"    ⚠️ Erros: {stats['errors']}")
    
    except Exception as e:
        print(f"⚠️ Erro Supabase: {e}")
    
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