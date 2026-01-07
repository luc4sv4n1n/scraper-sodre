#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SODRÉ SANTORO - SCRAPER COMPLETO
Suporta: Veículos + Imóveis + Materiais
"""

import sys
import json
import time
import random
import requests
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from supabase_client import SupabaseClient
from normalizer import normalize_items


class SodreScraper:
    
    def __init__(self):
        self.source = 'sodre'
        self.base_url = 'https://www.sodresantoro.com.br'
        self.api_url = f'{self.base_url}/api/search'
        
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
            (['direitos sobre apartamento', 'direitos sobre imóvel residencial', 
              'direitos sobre terreno', 'direitos sobre unidade autônoma', 
              'parte ideal de 1/6 sobre imóvel residencial', 
              'parte ideal de 50% sobre lote de terreno', 
              'parte ideal de 50% sobre nua-propriedade'], 
             'imoveis', 'Direitos e Partes Ideais', {'property_type': 'outros'}),
        ]
        
        # MÁQUINAS PESADAS E AGRÍCOLAS
        self.heavy_machinery_sections = [
            (['implementos agrícolas', 'terraplenagem', 'tratores'], 
             'maquinas_pesadas_agricolas', 'Máquinas Pesadas e Agrícolas', {}),
        ]
        
        # SUCATAS E RESÍDUOS (do índice materiais)
        self.scrap_sections = [
            (['sucata', 'veículos fora de estrada'], 
             'sucatas_residuos', 'Sucatas e Resíduos (Materiais)', {}),
        ]
        
        # SUCATAS (índice próprio - sem filtro de categoria)
        self.scrap_index_sections = [
            ([], 'sucatas_residuos', 'Sucatas (Índice Próprio)', {}),
        ]
        
        # BENS DE CONSUMO
        self.consumption_goods_sections = [
            (['uso pessoal'], 'bens_consumo', 'Uso Pessoal', {'consumption_goods_type': 'uso_pessoal'}),
            (['materiais escolares'], 'bens_consumo', 'Materiais Escolares', {'consumption_goods_type': 'materiais_escolares'}),
            (['infantil'], 'bens_consumo', 'Infantil', {'consumption_goods_type': 'infantil'}),
            (['brinquedos'], 'bens_consumo', 'Brinquedos', {'consumption_goods_type': 'brinquedos'}),
        ]
        
        # INDUSTRIAL EQUIPAMENTOS
        self.industrial_sections = [
            (['eletricos'], 'industrial_equipamentos', 'Elétricos', {}),
            (['empilhadeiras'], 'industrial_equipamentos', 'Empilhadeiras', {}),
            (['equip. e mat. industriais'], 'industrial_equipamentos', 'Equipamentos e Materiais Industriais', {}),
            (['móveis industriais'], 'industrial_equipamentos', 'Móveis Industriais', {}),
            (['maquinas de solda'], 'industrial_equipamentos', 'Máquinas de Solda', {}),
            (['balanças'], 'industrial_equipamentos', 'Balanças', {}),
        ]
        
        # MATERIAIS CONSTRUÇÃO
        self.construction_sections = [
            (['ferramentas'], 'materiais_construcao', 'Ferramentas', {'construction_material_type': 'ferramentas'}),
            (['construção civil'], 'materiais_construcao', 'Construção Civil', {'construction_material_type': 'materiais'}),
            (['casa / construção'], 'materiais_construcao', 'Casa e Construção', {'construction_material_type': 'materiais'}),
        ]
        
        # NICHADOS
        self.specialized_sections = [
            (['academia'], 'nichados', 'Academia', {'specialized_type': 'academia'}),
            (['topógrafo'], 'nichados', 'Topógrafo', {'specialized_type': 'topografo'}),
            (['lazer/esportes'], 'nichados', 'Lazer e Esportes', {'specialized_type': 'lazer'}),
            (['instrumentos musicais'], 'nichados', 'Instrumentos Musicais', {'specialized_type': 'instrumentos_musicais'}),
            (['bares, restaurantes e supermercados'], 'nichados', 'Bares, Restaurantes e Supermercados', {'specialized_type': 'restaurante'}),
        ]
        
        # TECNOLOGIA
        self.technology_sections = [
            (['telefonia e comunicação'], 'tecnologia', 'Telefonia e Comunicação', {'tech_type': 'telefonia'}),
            (['eletroeletrônicos'], 'tecnologia', 'Eletrônicos', {'tech_type': 'eletronicos'}),
            (['eletrodomesticos'], 'tecnologia', 'Eletrodomésticos', {'tech_type': 'eletrodomesticos'}),
        ]
        
        # MÓVEIS E DECORAÇÃO
        self.furniture_sections = [
            (['móveis p/ casa'], 'moveis_decoracao', 'Móveis para Casa', {}),
            (['moveis para escritório'], 'moveis_decoracao', 'Móveis para Escritório', {}),
            (['equip. e mat. p/ escritório'], 'moveis_decoracao', 'Equipamentos para Escritório', {}),
        ]
        
        # DIVERSOS (itens que não se encaixam em outras categorias)
        self.miscellaneous_sections = [
            (['diversos'], 'industrial_equipamentos', 'Diversos', {}),
        ]
        
        self.all_sections = (
            self.vehicle_sections + 
            self.property_sections + 
            self.heavy_machinery_sections +
            self.scrap_sections +
            self.scrap_index_sections +  # Sucatas com índice próprio
            self.consumption_goods_sections +
            self.industrial_sections +
            self.construction_sections +
            self.specialized_sections +
            self.technology_sections +
            self.furniture_sections +
            self.miscellaneous_sections
        )
        
        self.stats = {
            'total_scraped': 0,
            'by_table': defaultdict(int),
            'by_section': {},
            'duplicates': 0,
        }
        
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'pt-BR,pt;q=0.9',
            'Content-Type': 'application/json',
            'Origin': self.base_url,
            'Referer': f'{self.base_url}/veiculos/lotes',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def scrape(self) -> dict:
        print("\n" + "="*60)
        print("🟣 SODRÉ SANTORO - SCRAPER COMPLETO")
        print("="*60)
        
        items_by_table = defaultdict(list)
        global_ids = set()
        
        for lot_categories, table, display_name, extra_fields in self.all_sections:
            print(f"\n📦 {display_name} → {table}")
            
            section_items = self._scrape_section(
                lot_categories, table, display_name, extra_fields, global_ids
            )
            
            items_by_table[table].extend(section_items)
            section_key = '+'.join(lot_categories) if lot_categories else f'INDEX_{table}'
            self.stats['by_section'][section_key] = len(section_items)
            self.stats['by_table'][table] += len(section_items)
            
            print(f"✅ {len(section_items)} itens → {table}")
            time.sleep(2)
        
        self.stats['total_scraped'] = sum(len(items) for items in items_by_table.values())
        return items_by_table
    
    def _scrape_section(self, lot_categories: List[str], table: str,
                       display_name: str, extra_fields: dict,
                       global_ids: set) -> List[dict]:
        items = []
        page_num = 0
        page_size = 48
        consecutive_errors = 0
        max_errors = 3
        max_pages = 100
        
        while page_num < max_pages and consecutive_errors < max_errors:
            print(f"  Pág {page_num + 1}", end='', flush=True)
            
            try:
                payload = self._build_payload(lot_categories, page_num, page_size, table)
                
                response = self.session.post(self.api_url, json=payload, timeout=45)
                
                if response.status_code == 404:
                    print(f" ⚪ Fim")
                    break
                
                if response.status_code != 200:
                    print(f" ⚠️ Status {response.status_code}")
                    consecutive_errors += 1
                    time.sleep(5)
                    page_num += 1
                    continue
                
                data = response.json()
                hits = data.get('hits', {}).get('hits', [])
                
                if not hits:
                    print(f" ⚪ Vazia")
                    break
                
                novos = 0
                duplicados = 0
                
                for hit in hits:
                    source = hit.get('_source', {})
                    item = self._extract_lot(source, table, display_name, extra_fields)
                    
                    if not item:
                        continue
                    
                    if item['external_id'] in global_ids:
                        duplicados += 1
                        self.stats['duplicates'] += 1
                        continue
                    
                    items.append(item)
                    global_ids.add(item['external_id'])
                    novos += 1
                
                if novos > 0:
                    print(f" ✅ +{novos}")
                    consecutive_errors = 0
                else:
                    print(f" ⚪ 0 novos (dup: {duplicados})")
                
                total = data.get('hits', {}).get('total', {})
                if isinstance(total, dict):
                    total_value = total.get('value', 0)
                else:
                    total_value = total
                
                if (page_num + 1) * page_size >= total_value:
                    break
                
                page_num += 1
                time.sleep(random.uniform(2, 4))
                
            except requests.exceptions.JSONDecodeError:
                print(f" ⚠️ Erro JSON")
                consecutive_errors += 1
                time.sleep(5)
                page_num += 1
            
            except Exception as e:
                print(f" ❌ Erro: {str(e)[:80]}")
                consecutive_errors += 1
                time.sleep(5)
                page_num += 1
        
        return items
    
    def _build_payload(self, lot_categories: List[str], page_num: int, page_size: int, table: str = 'materiais') -> dict:
        # Determina os índices baseado na tabela
        if table == 'veiculos':
            indices = ["veiculos", "judiciais-veiculos"]
        elif table == 'imoveis':
            indices = ["imoveis", "judiciais-imoveis"]
        else:
            # Para todas as outras categorias (materiais, etc)
            indices = ["materiais", "judiciais-materiais"]
        
        return {
            "indices": indices,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "bool": {
                                "should": [
                                    {"bool": {"must": [{"term": {"auction_status": "online"}}]}},
                                    {"bool": {"must": [{"term": {"auction_status": "aberto"}}], "must_not": [{"terms": {"lot_status_id": [5, 7]}}]}},
                                    {"bool": {"must": [{"term": {"auction_status": "encerrado"}}, {"terms": {"lot_status_id": [6]}}]}}
                                ],
                                "minimum_should_match": 1
                            }
                        },
                        {
                            "bool": {
                                "should": [
                                    {"bool": {"must_not": {"term": {"lot_status_id": 6}}}},
                                    {"bool": {"must": [{"term": {"lot_status_id": 6}}, {"term": {"segment_id": 1}}]}}
                                ],
                                "minimum_should_match": 1
                            }
                        },
                        {
                            "bool": {
                                "should": [{"bool": {"must_not": [{"term": {"lot_test": True}}]}}],
                                "minimum_should_match": 1
                            }
                        }
                    ]
                }
            },
            "post_filter": {
                "bool": {
                    "filter": [{"terms": {"lot_category": lot_categories}}]
                }
            },
            "from": page_num * page_size,
            "size": page_size,
            "sort": [
                {"lot_status_id_order": {"order": "asc"}},
                {"auction_date_init": {"order": "asc"}}
            ]
        }
    
    def _extract_lot(self, source: dict, table: str, display_name: str, extra_fields: dict) -> Optional[dict]:
        try:
            lot_id = source.get('lot_id')
            auction_id = source.get('auction_id')
            
            if not lot_id:
                return None
            
            external_id = f"sodre_{lot_id}"
            
            title = source.get('lot_title', '').strip()
            if not title or len(title) < 3:
                return None
            
            description = source.get('lot_description', '')
            
            value = source.get('lot_value_initial')
            value_text = None
            if value:
                value_text = f"R$ {value:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            
            city = None
            state = None
            location = source.get('lot_location', '')
            if location and '/' in location:
                parts = location.split('/')
                city = parts[0].strip()
                state = parts[1].strip() if len(parts) > 1 else None
            
            link = f"{self.base_url}/leilao/{auction_id}/lote/{lot_id}" if auction_id else None
            auction_date = source.get('auction_date_init')
            auction_name = source.get('auction_name')
            lot_number = source.get('lot_number')
            client_name = source.get('client_name')
            
            lot_praca = source.get('lot_praca')
            lot_praca_label = source.get('lot_praca_label')
            
            auction_round = None
            discount_percentage = None
            first_round_value = None
            first_round_date = None
            
            if lot_praca and lot_praca > 1:
                auction_round = lot_praca
                first_value = source.get('lot_value_first_round')
                if first_value and value and first_value > 0:
                    first_round_value = first_value
                    discount_percentage = ((first_value - value) / first_value) * 100
            
            total_visits = source.get('lot_total_views', 0)
            total_bids = source.get('lot_total_bids', 0)
            
            metadata = {
                'secao_site': display_name,
                'categoria_original': source.get('lot_category'),
                'leilao_id': auction_id,
                'status_id': source.get('lot_status_id'),
                'praca_numero': lot_praca,
                'praca_label': lot_praca_label,
            }
            
            # Campos específicos de veículos e sucatas
            if table == 'veiculos' or (table == 'sucatas_residuos' and not extra_fields.get('construction_material_type')):
                metadata.update({
                    'marca': source.get('lot_brand'),
                    'modelo': source.get('lot_model'),
                    'ano_modelo': source.get('lot_year_model'),
                    'km': source.get('lot_km'),
                    'combustivel': source.get('lot_fuel'),
                    'cambio': source.get('lot_transmission'),
                    'origem': source.get('lot_origin'),
                    'sinistro': source.get('lot_sinister'),
                    'opcionais': source.get('lot_optionals', []),
                    'financiavel': source.get('lot_financeable'),
                    'placa': source.get('lot_plate'),
                    'chassi': source.get('lot_chassi'),
                    'renavam': source.get('lot_renavam'),
                })
            
            # Campos específicos de imóveis
            if table == 'imoveis':
                metadata.update({
                    'area_total': source.get('lot_area_total'),
                    'area_construida': source.get('lot_area_built'),
                    'quartos': source.get('lot_bedrooms'),
                    'banheiros': source.get('lot_bathrooms'),
                    'vagas': source.get('lot_parking_spaces'),
                    'ocupado': source.get('lot_occupied'),
                    'bairro': source.get('lot_neighborhood'),
                    'cep': source.get('lot_zipcode'),
                    'matricula': source.get('lot_registration'),
                    'iptu': source.get('lot_iptu'),
                })
            
            # Campos específicos de materiais (máquinas, equipamentos, etc)
            if table not in ['veiculos', 'imoveis']:
                metadata.update({
                    'marca': source.get('lot_brand'),
                    'modelo': source.get('lot_model'),
                    'subcategoria': source.get('lot_subcategory'),
                })
            
            metadata = {k: v for k, v in metadata.items() if v is not None}
            
            item = {
                'source': 'sodre',
                'external_id': external_id,
                'title': title,
                'description': description,
                'value': value,
                'value_text': value_text,
                'city': city,
                'state': state,
                'link': link,
                'target_table': table,
                'auction_date': auction_date,
                'auction_type': 'Leilão',
                'auction_name': auction_name,
                'store_name': client_name,
                'lot_number': str(lot_number) if lot_number else None,
                'total_visits': total_visits,
                'total_bids': total_bids,
                'total_bidders': 0,
                'auction_round': auction_round,
                'discount_percentage': round(discount_percentage, 2) if discount_percentage else None,
                'first_round_value': first_round_value,
                'first_round_date': first_round_date,
                'metadata': metadata,
            }
            
            # Adiciona campos extras (vehicle_type, property_type, consumption_goods_type, etc.)
            if extra_fields:
                item.update(extra_fields)
            
            # Filtra itens de teste
            if source.get('lot_test'):
                return None
            
            # Valor muito baixo (suspeito)
            if value and value < 1:
                return None
            
            return item
            
        except Exception as e:
            return None


def main():
    print("\n" + "="*70)
    print("🚀 SODRÉ SANTORO - SCRAPER COMPLETO")
    print("="*70)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = time.time()
    
    print("\n🔥 FASE 1: COLETANDO DADOS")
    scraper = SodreScraper()
    items_by_table = scraper.scrape()
    
    total_items = sum(len(items) for items in items_by_table.values())
    
    print(f"\n✅ Total coletado: {total_items} itens")
    print(f"📄 Duplicatas filtradas: {scraper.stats['duplicates']}")
    
    if not total_items:
        print("⚠️ Nenhum item coletado - encerrando")
        return
    
    print("\n✨ FASE 2: NORMALIZANDO DADOS")
    
    normalized_by_table = {}
    
    for table, items in items_by_table.items():
        if not items:
            continue
        
        try:
            normalized = normalize_items(items)
            normalized_by_table[table] = normalized
            print(f"  ✅ {table}: {len(normalized)} itens normalizados")
        except Exception as e:
            print(f"  ⚠️ Erro em {table}: {e}")
            normalized_by_table[table] = items
    
    output_dir = Path(__file__).parent / 'data' / 'normalized'
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_file = output_dir / f'sodre_{timestamp}.json'
    
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(normalized_by_table, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON salvo: {json_file}")
    
    print("\n📤 FASE 3: INSERINDO NO SUPABASE")
    
    try:
        supabase = SupabaseClient()
        
        if not supabase.test():
            print("⚠️ Erro na conexão com Supabase - pulando insert")
        else:
            total_inserted = 0
            total_updated = 0
            
            for table, items in normalized_by_table.items():
                if not items:
                    continue
                
                print(f"\n  📤 Tabela '{table}': {len(items)} itens")
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
        print(f"⚠️ Erro no Supabase: {e}")
    
    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    
    print("\n" + "="*70)
    print("📊 ESTATÍSTICAS FINAIS")
    print("="*70)
    print(f"🟣 Sodré Santoro:")
    print(f"\n  Por Tabela:")
    for table, count in sorted(scraper.stats['by_table'].items()):
        print(f"    • {table}: {count} itens")
    print(f"\n  • Total coletado: {scraper.stats['total_scraped']}")
    print(f"  • Duplicatas: {scraper.stats['duplicates']}")
    print(f"\n⏱️ Duração: {minutes}min {seconds}s")
    print(f"✅ Concluído: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)


if __name__ == "__main__":
    main()