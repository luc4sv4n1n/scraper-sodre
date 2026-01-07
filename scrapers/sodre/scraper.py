#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SODRÉ SANTORO - SCRAPER RÁPIDO (CATEGORIAS EXATAS DO SITE)
"""

import sys
import json
import time
import random
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import List, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

sys.path.insert(0, str(Path(__file__).parent.parent))

from supabase_client import SupabaseClient
from normalizer import normalize_items


class SodreScraper:
    
    def __init__(self):
        self.source = 'sodre'
        self.base_url = 'https://www.sodresantoro.com.br'
        self.leilao_base_url = 'https://leilao.sodresantoro.com.br'
        
        # VEÍCULOS
        self.vehicle_sections = [
            (['caminhões'], 'veiculos', 'Caminhões', {'vehicle_type': 'caminhao'}),
            (['utilit. pesados'], 'veiculos', 'Utilitários Pesados', {'vehicle_type': 'pesados'}),
            (['peruas'], 'veiculos', 'Peruas', {'vehicle_type': 'perua'}),
            (['onibus'], 'veiculos', 'Ônibus', {'vehicle_type': 'onibus'}),
            (['implementos rod.'], 'veiculos', 'Implementos Rodoviários', {'vehicle_type': 'implemento_rodoviario'}),
            (['van leve'], 'veiculos', 'Vans', {'vehicle_type': 'van'}),
            (['carros', 'utilitarios leves'], 'veiculos', 'Carros', {'vehicle_type': 'carro'}),
            (['motos'], 'veiculos', 'Motos', {'vehicle_type': 'moto'}),
            (['embarcações'], 'veiculos', 'Embarcações', {'vehicle_type': 'barco'}),
        ]
        
        # IMÓVEIS
        self.property_sections = [
            (['apartamento'], 'imoveis', 'Apartamentos', {'property_type': 'apartamento'}),
            (['galpão', 'galpão industrial'], 'imoveis', 'Imóveis Industriais', {'property_type': 'galpao_industrial'}),
            (['imóvel residencial', 'imóvel residencial com 3 edificações', 'imóvel residencial tipo sobrado'], 
             'imoveis', 'Imóveis Residenciais', {'property_type': 'residencial'}),
            (['lote de terreno', 'terreno urbano', 'área de terras'], 
             'imoveis', 'Terrenos e Lotes', {'property_type': 'terreno_lote'}),
            (['prédio comercial', 'sala comercial'], 
             'imoveis', 'Imóveis Comerciais', {'property_type': 'comercial'}),
            (['direitos sobre apartamento', 'direitos sobre imóvel residencial'], 
             'imoveis', 'Direitos e Partes Ideais', {'property_type': 'outros'}),
        ]
        
        # MATERIAIS - CATEGORIAS EXATAS DAS URLs REAIS
        self.materials_sections = [
            # MÁQUINAS PESADAS E AGRÍCOLAS
            (['implementos agrícolas', 'terraplenagem', 'tratores'], 
             'maquinas_pesadas_agricolas', 'Máquinas Pesadas e Agrícolas', {}),
            
            # SUCATAS - /materiais/ com filtro
            (['sucata', 'veículos fora de estrada'], 
             'sucatas_residuos', 'Sucatas e Resíduos (Materiais)', {}),
            
            # BENS DE CONSUMO
            (['uso pessoal', 'materiais escolares', 'infantil', 'brinquedos'], 
             'bens_consumo', 'Bens de Consumo', {'consumption_goods_type': 'uso_pessoal'}),
            
            # INDUSTRIAL E EQUIPAMENTOS
            (['eletricos', 'empilhadeiras', 'equip. e mat. industriais', 'maquinas de solda', 'móveis industriais', 'balanças'], 
             'industrial_equipamentos', 'Industrial e Equipamentos', {}),
            
            # MATERIAIS DE CONSTRUÇÃO - tipo casa
            (['casa / construção'], 
             'materiais_construcao', 'Casa e Construção', {'construction_material_type': 'materiais'}),
            
            # MATERIAIS DE CONSTRUÇÃO - tipo ferramentas
            (['ferramentas'], 
             'materiais_construcao', 'Ferramentas', {'construction_material_type': 'ferramentas'}),
            
            # MATERIAIS DE CONSTRUÇÃO - tipo diversos
            (['construção civil'], 
             'materiais_construcao', 'Construção Civil', {'construction_material_type': 'diversos'}),
            
            # NICHADOS - tipo negocios
            (['equip. e mat. p/ escritório'], 
             'nichados', 'Equipamentos para Negócios', {'specialized_type': 'negocios'}),
            
            # NICHADOS - tipo academia
            (['academia'], 
             'nichados', 'Academia', {'specialized_type': 'academia'}),
            
            # NICHADOS - tipo bar-restaurante-mercado
            (['bares, restaurantes e supermercados'], 
             'nichados', 'Bares e Restaurantes', {'specialized_type': 'restaurante'}),
            
            # NICHADOS - outros
            (['instrumentos musicais', 'lazer/esportes', 'topógrafo'], 
             'nichados', 'Outros Nichados', {'specialized_type': 'lazer'}),
            
            # TECNOLOGIA - tipo diversos
            (['telefonia e comunicação', 'eletrodomesticos', 'eletroeletrônicos'], 
             'tecnologia', 'Tecnologia Diversos', {'tech_type': 'diversos'}),
            
            # TECNOLOGIA - tipo informatica
            (['informatica'], 
             'tecnologia', 'Informática', {'tech_type': 'informatica'}),
            
            # MÓVEIS E DECORAÇÃO
            (['moveis para escritório', 'móveis p/ casa'], 
             'moveis_decoracao', 'Móveis e Decoração', {}),
        ]
        
        # SUCATAS - índice próprio (sem filtro)
        self.sucatas_index = [
            ([], 'sucatas_residuos', 'Sucatas (Índice)', {}),
        ]
        
        self.all_sections = (
            self.vehicle_sections + 
            self.property_sections + 
            self.materials_sections +
            self.sucatas_index
        )
        
        self.stats = {
            'total_scraped': 0,
            'by_table': defaultdict(int),
            'by_section': {},
            'duplicates': 0,
            'empty_sections': 0,
            'errors': 0,
        }
    
    def _get_section_type(self, table: str) -> str:
        """Mapeia tabela para seção do site"""
        mapping = {
            'veiculos': 'veiculos',
            'imoveis': 'imoveis',
            'sucatas_residuos': 'materiais',  # Primeiro tenta /materiais/, depois /sucatas/
        }
        return mapping.get(table, 'materiais')
    
    def _build_category_param(self, categories: List[str]) -> str:
        """Converte lista de categorias para formato da URL"""
        if len(categories) == 1:
            return categories[0].replace(' ', '+')
        else:
            formatted = []
            for cat in categories:
                formatted.append(cat.replace(' ', '+'))
            return '__'.join(formatted)
    
    def _build_url(self, section_type: str, categories: List[str], display_name: str) -> str:
        """Constrói URL com filtros de categoria"""
        # CASO ESPECIAL: Sucatas índice próprio
        if 'Índice' in display_name:
            return f"{self.base_url}/sucatas/lotes?sort=auction_date_init_asc"
        
        base = f"{self.base_url}/{section_type}/lotes"
        
        if not categories:
            return f"{base}?sort=auction_date_init_asc"
        
        category_param = self._build_category_param(categories)
        return f"{base}?lot_category={category_param}&sort=auction_date_init_asc"
    
    def scrape(self) -> dict:
        print("\n" + "="*60)
        print("🟣 SODRÉ SANTORO - SCRAPER RÁPIDO")
        print("="*60)
        
        items_by_table = defaultdict(list)
        global_ids = set()
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
            
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
                locale='pt-BR',
            )
            
            page = context.new_page()
            
            for lot_categories, table, display_name, extra_fields in self.all_sections:
                print(f"\n📦 {display_name} → {table}")
                
                section_items = self._scrape_section(
                    page, lot_categories, table, display_name, extra_fields, global_ids
                )
                
                if len(section_items) == 0:
                    self.stats['empty_sections'] += 1
                
                items_by_table[table].extend(section_items)
                section_key = '+'.join(lot_categories) if lot_categories else f'INDEX_{table}'
                self.stats['by_section'][section_key] = len(section_items)
                self.stats['by_table'][table] += len(section_items)
                
                print(f"✅ {len(section_items)} itens → {table}")
                time.sleep(random.uniform(2, 4))
            
            browser.close()
        
        self.stats['total_scraped'] = sum(len(items) for items in items_by_table.values())
        return items_by_table
    
    def _scrape_section(self, page, lot_categories: List[str], table: str,
                       display_name: str, extra_fields: dict,
                       global_ids: set) -> List[dict]:
        items = []
        section_type = self._get_section_type(table)
        url = self._build_url(section_type, lot_categories, display_name)
        
        print(f"  🌐 {url[:90]}...")
        
        try:
            page.goto(url, wait_until='domcontentloaded', timeout=30000)
            time.sleep(random.uniform(6, 8))
            
            try:
                page.wait_for_selector('a[href*="/leilao/"]', timeout=10000)
            except:
                print(f"  ⚪ Sem lotes encontrados")
                return items
            
            # Loop de paginação
            current_page = 1
            max_pages = 20
            
            while current_page <= max_pages:
                page_items = self._extract_lots_from_page(page, table, display_name, extra_fields, global_ids)
                items.extend(page_items)
                
                print(f"  📄 Pág {current_page}: {len(items)} itens", flush=True)
                
                next_button = page.query_selector('button[title="Avançar"]:not([disabled])')
                
                if not next_button or current_page >= max_pages:
                    break
                
                try:
                    next_button.click()
                    time.sleep(random.uniform(3, 5))
                    page.wait_for_selector('a[href*="/leilao/"]', timeout=10000)
                    time.sleep(random.uniform(2, 3))
                    current_page += 1
                except Exception as e:
                    print(f"\n  ⚠️ Erro ao paginar: {str(e)[:50]}")
                    break
            
        except PlaywrightTimeout:
            print(f"  ⏱️ Timeout")
            self.stats['errors'] += 1
        except Exception as e:
            print(f"  ❌ Erro: {str(e)[:50]}")
            self.stats['errors'] += 1
        
        return items
    
    def _extract_lots_from_page(self, page, table: str, display_name: str, 
                                extra_fields: dict, global_ids: set) -> List[dict]:
        items = []
        
        try:
            cards = page.query_selector_all('a[href*="/leilao/"][href*="/lote/"]')
            
            if not cards:
                return items
            
            for card in cards:
                try:
                    item = self._extract_lot_from_card(card, table, display_name, extra_fields)
                    
                    if not item:
                        continue
                    
                    if item['external_id'] in global_ids:
                        self.stats['duplicates'] += 1
                        continue
                    
                    items.append(item)
                    global_ids.add(item['external_id'])
                    
                except Exception as e:
                    continue
        
        except Exception as e:
            pass
        
        return items
    
    def _extract_lot_from_card(self, card, table: str, display_name: str, 
                              extra_fields: dict) -> Optional[dict]:
        try:
            link = card.get_attribute('href')
            if not link:
                return None
            
            if not link.startswith('http'):
                link = f"{self.leilao_base_url}{link}"
            
            match = re.search(r'/leilao/(\d+)/lote/(\d+)', link)
            if not match:
                return None
            
            auction_id = match.group(1)
            lot_id = match.group(2)
            external_id = f"sodre_{lot_id}"
            
            card_text = card.inner_text()
            
            # Título
            title = None
            title_elem = card.query_selector('.text-body-medium')
            if title_elem:
                title = title_elem.inner_text().strip()
            
            if not title or len(title) < 3:
                return None
            
            # Valor
            value = None
            value_text = None
            value_elem = card.query_selector('.text-primary.text-headline-small')
            if value_elem:
                text = value_elem.inner_text().strip()
                match = re.search(r'([\d.,]+)', text)
                if match:
                    value_str = match.group(1).replace('.', '').replace(',', '.')
                    try:
                        value = float(value_str)
                        value_text = f"R$ {value:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                    except:
                        pass
            
            # Localização
            city = None
            state = None
            location_items = card.query_selector_all('li')
            for li in location_items:
                text = li.inner_text().strip()
                if '/' in text and len(text) < 50:
                    parts = text.split('/')
                    if len(parts) == 2:
                        city = parts[0].strip()
                        state = parts[1].strip()
                        break
            
            # Número do lote
            lot_number = None
            auction_info = card.query_selector('.text-label-small')
            if auction_info:
                text = auction_info.inner_text().strip()
                match = re.search(r'-\s*(\d+)', text)
                if match:
                    lot_number = match.group(1)
            
            # Comitente
            store_name = None
            store_elem = card.query_selector('.text-body-small.text-on-surface-variant.uppercase.line-clamp-1')
            if store_elem:
                store_name = store_elem.inner_text().strip()
                if not store_name or len(store_name) <= 2:
                    store_name = None
            
            # Data
            auction_date = None
            date_elem = card.query_selector('.text-body-small.line-clamp-1')
            if date_elem:
                date_text = date_elem.inner_text().strip()
                date_match = re.search(r'(\d{2})/(\d{2})/(\d{2})', date_text)
                if date_match:
                    day, month, year = date_match.groups()
                    year = f"20{year}"
                    auction_date = f"{year}-{month}-{day}"
            
            # Visitas
            total_visits = 0
            visits_elem = card.query_selector('.inline-flex.items-center.gap-x-1.text-label-small')
            if visits_elem:
                visits_text = visits_elem.inner_text().strip()
                visits_match = re.search(r'(\d+)', visits_text)
                if visits_match:
                    try:
                        total_visits = int(visits_match.group(1))
                    except:
                        pass
            
            # Metadata
            metadata = {
                'secao_site': display_name,
                'leilao_id': auction_id,
            }
            
            # Categoria
            list_items = card.query_selector_all('li span.line-clamp-1.text-body-small')
            if list_items and len(list_items) > 0:
                categoria_lote = list_items[0].inner_text().strip()
                if categoria_lote and len(categoria_lote) > 2:
                    metadata['categoria_lote'] = categoria_lote
            
            # Marca/modelo/ano (veículos e sucatas)
            if table == 'veiculos' or table == 'sucatas_residuos':
                for span in list_items:
                    text = span.inner_text().strip().lower()
                    if '-' in text and '/' not in text:
                        marca = text.replace('-', '').strip()
                        if marca:
                            metadata['marca'] = marca
                            break
                
                brand_match = re.search(r'([A-Z][A-Z\s]+?)\s+([A-Z0-9][A-Z0-9\s/-]+?)\s+(\d{2}/\d{2}|\d{4})', title.upper())
                if brand_match:
                    if 'marca' not in metadata:
                        metadata['marca'] = brand_match.group(1).strip()
                    metadata['modelo'] = brand_match.group(2).strip()
                    year_str = brand_match.group(3)
                    if '/' in year_str:
                        metadata['ano_modelo'] = year_str.split('/')[0]
                    else:
                        metadata['ano_modelo'] = year_str
            
            # Área (imóveis)
            if table == 'imoveis':
                area_match = re.search(r'(\d+(?:[.,]\d+)?)\s*m[²2]', card_text)
                if area_match:
                    area_str = area_match.group(1).replace(',', '.')
                    try:
                        metadata['area_total'] = float(area_str)
                    except:
                        pass
            
            # Marca (outros)
            if table not in ['veiculos', 'imoveis', 'sucatas_residuos']:
                for span in list_items:
                    text = span.inner_text().strip().lower()
                    if '-' in text and '/' not in text and len(text) < 30:
                        marca = text.replace('-', '').strip()
                        if marca:
                            metadata['marca'] = marca
                            break
            
            metadata = {k: v for k, v in metadata.items() if v is not None}
            
            item = {
                'source': 'sodre',
                'external_id': external_id,
                'title': title,
                'description': None,
                'value': value,
                'value_text': value_text,
                'city': city,
                'state': state,
                'link': link,
                'target_table': table,
                'auction_date': auction_date,
                'auction_type': 'Leilão',
                'auction_name': None,
                'store_name': store_name,
                'lot_number': lot_number,
                'total_visits': total_visits,
                'total_bids': 0,
                'total_bidders': 0,
                'auction_round': None,
                'discount_percentage': None,
                'first_round_value': None,
                'metadata': metadata,
            }
            
            if extra_fields:
                item.update(extra_fields)
            
            return item
            
        except Exception as e:
            return None


def main():
    print("\n" + "="*70)
    print("🚀 SODRÉ SANTORO - SCRAPER RÁPIDO")
    print("="*70)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = time.time()
    
    print("\n🔥 FASE 1: COLETANDO DADOS")
    scraper = SodreScraper()
    items_by_table = scraper.scrape()
    
    total_items = sum(len(items) for items in items_by_table.values())
    
    print(f"\n✅ Total coletado: {total_items} itens")
    print(f"🔄 Duplicatas: {scraper.stats['duplicates']}")
    print(f"⚪ Seções vazias: {scraper.stats['empty_sections']}")
    print(f"❌ Erros: {scraper.stats['errors']}")
    
    if not total_items:
        print("⚠️ Nenhum item coletado")
        return
    
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
    
    output_dir = Path(__file__).parent / 'data' / 'normalized'
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_file = output_dir / f'sodre_{timestamp}.json'
    
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(normalized_by_table, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON: {json_file}")
    
    print("\n📤 FASE 3: SUPABASE")
    
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
            
            print(f"\n  ✅ Total inseridos: {total_inserted}")
            print(f"  🔄 Total atualizados: {total_updated}")
    
    except Exception as e:
        print(f"⚠️ Erro Supabase: {e}")
    
    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    
    print("\n" + "="*70)
    print("📊 ESTATÍSTICAS")
    print("="*70)
    for table, count in sorted(scraper.stats['by_table'].items()):
        print(f"  • {table}: {count}")
    print(f"\n⏱️ Duração: {minutes}min {seconds}s")
    print("="*70)


if __name__ == "__main__":
    main()