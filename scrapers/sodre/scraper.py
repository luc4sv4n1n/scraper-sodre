#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SODRÉ SANTORO - SCRAPER COM INTERCEPTAÇÃO PASSIVA
✅ Mapeamento direto para tabela sodre_items
✅ Sem normalizer - campos diretos da API
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
    
    def __init__(self):
        self.source = 'sodre'
        self.base_url = 'https://www.sodresantoro.com.br'
        self.leilao_base_url = 'https://leilao.sodresantoro.com.br'
        
        # URLs para scraping (todas as categorias)
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
    
    async def scrape(self) -> List[Dict]:
        """Scrape completo com interceptação passiva"""
        print("\n" + "="*60)
        print("🟣 SODRÉ SANTORO - INTERCEPTAÇÃO PASSIVA")
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
            
            # Intercepta API
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
            
            # Navega URLs
            for url in self.urls:
                section_name = url.split('/')[3]
                print(f"\n📦 {section_name.upper()}")
                
                try:
                    await page.goto(url, wait_until="networkidle", timeout=60000)
                    await asyncio.sleep(3)
                    
                    # Paginação
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
            
            await browser.close()
        
        print(f"\n✅ {len(all_lots)} lotes capturados")
        
        # Processa lotes
        items = self._process_lots(all_lots)
        
        self.stats['total_scraped'] = len(items)
        return items
    
    def _process_lots(self, lots: List[Dict]) -> List[Dict]:
        """Processa lotes da API e mapeia para sodre_items"""
        print("\n📋 Processando lotes...")
        
        items = []
        seen_ids = set()
        
        for lot in lots:
            try:
                item = self._extract_lot_data(lot)
                
                if not item:
                    self.stats['errors'] += 1
                    continue
                
                if item['external_id'] in seen_ids:
                    self.stats['duplicates'] += 1
                    continue
                
                items.append(item)
                seen_ids.add(item['external_id'])
                
                if item.get('has_bid'):
                    self.stats['with_bids'] += 1
                
            except Exception as e:
                self.stats['errors'] += 1
                continue
        
        print(f"  ✅ {len(items)} itens válidos")
        return items
    
    def _extract_lot_data(self, lot: Dict) -> dict:
        """Extrai dados de um lote da API e mapeia para sodre_items"""
        try:
            # IDs obrigatórios
            auction_id = lot.get('auction_id')
            lot_id = lot.get('lot_id')
            
            if not auction_id or not lot_id:
                return None
            
            external_id = f"sodre_{int(lot_id)}"
            
            # Título obrigatório
            title = lot.get('lot_title', '').strip()
            if not title or len(title) < 3:
                return None
            
            # Link
            link = f"{self.leilao_base_url}/leilao/{auction_id}/lote/{lot_id}/"
            
            # Datas (timestamp with time zone)
            auction_date_init = self._parse_datetime(lot.get('auction_date_init'))
            auction_date_2 = self._parse_datetime(lot.get('auction_date_2'))
            auction_date_end = self._parse_datetime(lot.get('auction_date_end'))
            
            # Localização (city/state serão extraídos pelo trigger)
            lot_location = lot.get('lot_location', '').strip() or None
            
            # Imagem (primeira da lista)
            image_url = None
            lot_pictures = lot.get('lot_pictures', [])
            if lot_pictures and isinstance(lot_pictures, list) and len(lot_pictures) > 0:
                image_url = lot_pictures[0]
            
            # Optionals (array de texto)
            lot_optionals = lot.get('lot_optionals')
            if lot_optionals and isinstance(lot_optionals, list):
                lot_optionals = [str(opt) for opt in lot_optionals if opt]
            else:
                lot_optionals = None
            
            # Metadata adicional
            metadata = {
                'segment_id': lot.get('segment_id'),
                'segment_label': lot.get('segment_label'),
                'segment_slug': lot.get('segment_slug'),
                'lot_pictures': lot_pictures if lot_pictures else None,
                'search_terms': lot.get('search_terms'),
            }
            
            # Remove None do metadata
            metadata = {k: v for k, v in metadata.items() if v is not None}
            
            # Monta item com mapeamento direto para sodre_items
            item = {
                'external_id': external_id,
                'lot_id': int(lot_id),
                'lot_number': lot.get('lot_number', '').strip() or None,
                'lot_inspection_number': lot.get('lot_inspection_number', '').strip() or None,
                'lot_inspection_id': self._parse_int(lot.get('lot_inspection_id')),
                'auction_id': int(auction_id),
                'category': lot.get('category', '').strip() or None,
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
                'metadata': metadata,
                'is_active': True,
                'has_bid': bool(lot.get('bid_has_bid', False)),
            }
            
            return item
            
        except Exception as e:
            return None
    
    def _parse_datetime(self, value) -> str:
        """Converte datetime para ISO 8601 com timezone"""
        if not value:
            return None
        
        try:
            if isinstance(value, str):
                # Remove timezone se existir e adiciona +00:00
                value = value.replace('Z', '+00:00')
                
                # Tenta parse
                if 'T' in value:
                    return value
                else:
                    # Formato: 2026-01-13 14:00:00
                    dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                    return dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')
            return None
        except:
            return None
    
    def _parse_numeric(self, value):
        """Converte para numeric (float)"""
        if value is None:
            return None
        
        try:
            return float(value)
        except:
            return None
    
    def _parse_int(self, value):
        """Converte para integer"""
        if value is None:
            return None
        
        try:
            return int(value)
        except:
            return None


async def main():
    """Execução principal"""
    print("\n" + "="*70)
    print("🚀 SODRÉ SANTORO - SCRAPER")
    print("="*70)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = time.time()
    
    # FASE 1: SCRAPE
    print("\n🔥 FASE 1: COLETANDO DADOS")
    scraper = SodreScraper()
    items = await scraper.scrape()
    
    print(f"\n✅ Total coletado: {len(items)} itens")
    print(f"🔥 Itens com lances: {scraper.stats['with_bids']}")
    print(f"🔄 Duplicatas: {scraper.stats['duplicates']}")
    print(f"⚠️  Erros: {scraper.stats['errors']}")
    
    if not items:
        print("⚠️ Nenhum item coletado")
        return
    
    # Salva JSON
    output_dir = Path(__file__).parent / 'data' / 'normalized'
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_file = output_dir / f'sodre_{timestamp}.json'
    
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON: {json_file}")
    
    # FASE 2: SUPABASE
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
    
    # ESTATÍSTICAS
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