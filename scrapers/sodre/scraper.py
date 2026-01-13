#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SODRÉ SANTORO - SCRAPER COM INTERCEPTAÇÃO PASSIVA (VERSÃO SIMPLIFICADA)
✅ Usa has_bid (boolean) da API ao invés de total_bids
✅ API Sodré retorna bid_has_bid diretamente
"""

import asyncio
import sys
import json
import time
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import List, Dict
from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).parent.parent))

from supabase_client import SupabaseClient
from normalizer import normalize_items


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
        
        # Mapeamento de categorias (igual ao original)
        self.category_mapping = {
            # VEÍCULOS
            'caminhões': ('veiculos', {'vehicle_type': 'caminhao'}),
            'utilit. pesados': ('veiculos', {'vehicle_type': 'pesados'}),
            'peruas': ('veiculos', {'vehicle_type': 'perua'}),
            'onibus': ('veiculos', {'vehicle_type': 'onibus'}),
            'ônibus': ('veiculos', {'vehicle_type': 'onibus'}),
            'implementos rod.': ('veiculos', {'vehicle_type': 'implemento_rodoviario'}),
            'van leve': ('veiculos', {'vehicle_type': 'van'}),
            'carros': ('veiculos', {'vehicle_type': 'carro'}),
            'utilitarios leves': ('veiculos', {'vehicle_type': 'carro'}),
            'motos': ('veiculos', {'vehicle_type': 'moto'}),
            'embarcações': ('veiculos', {'vehicle_type': 'barco'}),
            
            # IMÓVEIS
            'apartamento': ('imoveis', {'property_type': 'apartamento'}),
            'apartamentos': ('imoveis', {'property_type': 'apartamento'}),
            'galpão': ('imoveis', {'property_type': 'galpao_industrial'}),
            'galpão industrial': ('imoveis', {'property_type': 'galpao_industrial'}),
            'imóvel residencial': ('imoveis', {'property_type': 'residencial'}),
            'imóveis residenciais': ('imoveis', {'property_type': 'residencial'}),
            'lote de terreno': ('imoveis', {'property_type': 'terreno_lote'}),
            'terreno urbano': ('imoveis', {'property_type': 'terreno_lote'}),
            'terrenos e lotes': ('imoveis', {'property_type': 'terreno_lote'}),
            'prédio comercial': ('imoveis', {'property_type': 'comercial'}),
            'sala comercial': ('imoveis', {'property_type': 'comercial'}),
            
            # MÁQUINAS
            'implementos agrícolas': ('maquinas_pesadas_agricolas', {}),
            'terraplenagem': ('maquinas_pesadas_agricolas', {}),
            'tratores': ('maquinas_pesadas_agricolas', {}),
            'eletricos': ('industrial_equipamentos', {}),
            'elétricos': ('industrial_equipamentos', {}),
            'empilhadeiras': ('industrial_equipamentos', {}),
            'equipamentos industriais': ('industrial_equipamentos', {}),
            
            # ELETRODOMÉSTICOS
            'ar condicionado': ('eletrodomesticos', {'appliance_type': 'climatizacao'}),
            'hidraulicos': ('eletrodomesticos', {'appliance_type': 'hidraulicos'}),
            'eletrodomesticos': ('eletrodomesticos', {'appliance_type': 'diversos'}),
            
            # CONSTRUÇÃO
            'casa / construção': ('materiais_construcao', {'construction_material_type': 'materiais'}),
            'ferramentas': ('materiais_construcao', {'construction_material_type': 'ferramentas'}),
            
            # NICHADOS
            'equipamentos para escritório': ('nichados', {'specialized_type': 'negocios'}),
            'academia': ('nichados', {'specialized_type': 'academia'}),
            'bares': ('nichados', {'specialized_type': 'restaurante'}),
            'restaurantes': ('nichados', {'specialized_type': 'restaurante'}),
            
            # TECNOLOGIA
            'telefonia': ('tecnologia', {'tech_type': 'telefonia'}),
            'eletrônicos': ('tecnologia', {'tech_type': 'eletronicos'}),
            'informatica': ('tecnologia', {'tech_type': 'informatica'}),
            
            # MÓVEIS
            'móveis para escritório': ('moveis_decoracao', {}),
            'móveis para casa': ('moveis_decoracao', {}),
            
            # BENS DE CONSUMO
            'uso pessoal': ('bens_consumo', {'consumption_goods_type': 'uso_pessoal'}),
            
            # SUCATAS
            'sucata': ('sucatas_residuos', {}),
            'veículos fora de estrada': ('sucatas_residuos', {}),
            
            # DIVERSOS
            'diversos': ('diversos', {}),
        }
        
        self.stats = {
            'total_scraped': 0,
            'by_table': defaultdict(int),
            'duplicates': 0,
            'with_bids': 0,  # ✅ Contador de itens com lances
            'unmapped_categories': set(),
        }
    
    async def scrape(self) -> dict:
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
        items_by_table = await self._process_lots(all_lots)
        
        self.stats['total_scraped'] = sum(len(items) for items in items_by_table.values())
        return items_by_table
    
    async def _process_lots(self, lots: List[Dict]) -> dict:
        """Processa lotes da API"""
        print("\n📋 Processando lotes...")
        
        items_by_table = defaultdict(list)
        global_ids = set()
        
        for lot in lots:
            try:
                item = self._extract_lot_data(lot)
                
                if not item:
                    continue
                
                if item['external_id'] in global_ids:
                    self.stats['duplicates'] += 1
                    continue
                
                table = item['target_table']
                items_by_table[table].append(item)
                global_ids.add(item['external_id'])
                self.stats['by_table'][table] += 1
                
                # ✅ Conta itens com lances
                if item.get('has_bid'):
                    self.stats['with_bids'] += 1
                
            except:
                continue
        
        print(f"\n📊 Itens por tabela:")
        for table, count in sorted(self.stats['by_table'].items()):
            print(f"  • {table}: {count}")
        
        return items_by_table
    
    def _extract_lot_data(self, lot: Dict) -> dict:
        """Extrai dados de um lote da API"""
        try:
            auction_id = lot.get('auction_id')
            lot_id = lot.get('lot_id')
            
            if not auction_id or not lot_id:
                return None
            
            link = f"{self.leilao_base_url}/leilao/{auction_id}/lote/{lot_id}/"
            external_id = f"sodre_{int(lot_id)}"
            
            title = lot.get('lot_title', '').strip()
            if not title or len(title) < 3:
                return None
            
            # Categoria → Tabela
            title_upper = title.upper()
            if 'SUCATA' in title_upper or 'SUCATAS' in title_upper:
                table = 'sucatas_residuos'
                extra_fields = {}
            else:
                lot_category = (lot.get('lot_category') or '').lower().strip()
                table_info = self.category_mapping.get(lot_category, ('diversos', {}))
                table, extra_fields = table_info
            
            # Valor
            value = None
            value_text = None
            bid_actual = lot.get('bid_actual')
            if bid_actual:
                try:
                    value = float(bid_actual)
                    value_text = f"R$ {value:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                except:
                    pass
            
            # Localização
            city = None
            state = None
            lot_city = lot.get('lot_location', '')  # ✅ Corrigido: lot_location
            
            if '/' in lot_city:
                parts = lot_city.split('/')
                city = parts[0].strip()
                state = parts[1].strip() if len(parts) > 1 else None
            elif lot_city:
                city = lot_city.strip()
            
            # Data
            auction_date = None
            date_str = lot.get('auction_date_init')
            if date_str:
                try:
                    auction_date = date_str.split('T')[0]
                except:
                    pass
            
            # ✅ HAS_BID - DIRETO DA API (boolean)
            has_bid = lot.get('bid_has_bid', False)
            
            # Metadata
            metadata = {
                'secao_site': lot.get('lot_category', '').strip(),
                'leilao_id': str(auction_id),
            }
            
            # Marca/modelo/ano (veículos e sucatas)
            if table == 'veiculos' or table == 'sucatas_residuos':
                brand = lot.get('lot_brand', '').strip()
                model = lot.get('lot_model', '').strip()
                year = lot.get('lot_year_model')  # ✅ Corrigido
                
                if brand:
                    metadata['marca'] = brand
                if model:
                    metadata['modelo'] = model
                if year:
                    metadata['ano_modelo'] = str(year)
            
            # Remove None
            metadata = {k: v for k, v in metadata.items() if v}
            
            # Monta item
            item = {
                'source': 'sodre',
                'external_id': external_id,
                'title': title,
                'description': lot.get('lot_description', ''),
                'value': value,
                'value_text': value_text,
                'city': city,
                'state': state,
                'link': link,
                'target_table': table,
                
                'auction_date': auction_date,
                'auction_type': 'Leilão',
                'store_name': lot.get('client_name', '').strip() or None,
                'lot_number': lot.get('lot_number', '').strip(),
                
                # ✅ HAS_BID (boolean)
                'has_bid': has_bid,
                
                'metadata': metadata,
            }
            
            if extra_fields:
                item.update(extra_fields)
            
            return item
            
        except:
            return None


async def main():
    """Execução principal"""
    print("\n" + "="*70)
    print("🚀 SODRÉ SANTORO - SCRAPER SIMPLIFICADO")
    print("="*70)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = time.time()
    
    # FASE 1: SCRAPE
    print("\n🔥 FASE 1: COLETANDO DADOS")
    scraper = SodreScraper()
    items_by_table = await scraper.scrape()
    
    total_items = sum(len(items) for items in items_by_table.values())
    
    print(f"\n✅ Total coletado: {total_items} itens")
    print(f"🔥 Itens com lances: {scraper.stats['with_bids']}")
    print(f"🔄 Duplicatas: {scraper.stats['duplicates']}")
    
    if not total_items:
        print("⚠️ Nenhum item coletado")
        return
    
    # FASE 2: NORMALIZAÇÃO
    print("\n✨ FASE 2: NORMALIZANDO DADOS")
    
    normalized_by_table = {}
    
    for table, items in items_by_table.items():
        if not items:
            continue
        
        try:
            normalized = normalize_items(items)
            normalized_by_table[table] = normalized
            print(f"  ✅ {table}: {len(normalized)} itens")
        except Exception as e:
            print(f"  ⚠️ Erro em {table}: {e}")
            normalized_by_table[table] = items
    
    # Salva JSON
    output_dir = Path(__file__).parent / 'data' / 'normalized'
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_file = output_dir / f'sodre_{timestamp}.json'
    
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(normalized_by_table, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON: {json_file}")
    
    # FASE 3: SUPABASE
    print("\n📤 FASE 3: INSERINDO NO SUPABASE")
    
    try:
        supabase = SupabaseClient()
        
        if not supabase.test():
            print("⚠️ Erro no Supabase")
        else:
            total_inserted = 0
            total_updated = 0
            
            for table, items in normalized_by_table.items():
                if not items:
                    continue
                
                print(f"\n  📤 {table}: {len(items)} itens")
                stats = supabase.upsert(table, items)
                
                print(f"    ✅ Inseridos: {stats['inserted']}")
                print(f"    🔄 Atualizados: {stats['updated']}")
                if stats['errors'] > 0:
                    print(f"    ⚠️ Erros: {stats['errors']}")
                
                total_inserted += stats['inserted']
                total_updated += stats['updated']
            
            print(f"\n  📈 TOTAL:")
            print(f"    ✅ Inseridos: {total_inserted}")
            print(f"    🔄 Atualizados: {total_updated}")
    
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
    print(f"\n  Por Tabela:")
    for table, count in sorted(scraper.stats['by_table'].items()):
        print(f"    • {table}: {count}")
    print(f"\n  • Total coletado: {scraper.stats['total_scraped']}")
    print(f"  • Com lances: {scraper.stats['with_bids']}")
    print(f"  • Duplicatas: {scraper.stats['duplicates']}")
    print(f"\n⏱️ Duração: {minutes}min {seconds}s")
    print(f"✅ Concluído: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)


if __name__ == "__main__":
    asyncio.run(main())