#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPABASE CLIENT - Genérico para todas as tabelas
Suporta campos especiais: vehicle_type, property_type, animal_type
"""

import os
import time
import requests
from datetime import datetime


class SupabaseClient:
    """Cliente para Supabase - Schema auctions"""
    
    def __init__(self):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("❌ Configure SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY")
        
        self.url = self.url.rstrip('/')
        
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Content-Profile': 'auctions',
            'Accept-Profile': 'auctions',
            'Prefer': 'resolution=merge-duplicates,return=minimal'
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def upsert(self, tabela: str, items: list) -> dict:
        """Upsert com suporte a campos específicos por tabela"""
        if not items:
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        prepared = []
        for item in items:
            try:
                db_item = self._prepare(item, tabela)
                if db_item:
                    prepared.append(db_item)
            except Exception as e:
                print(f"  ⚠️ Erro ao preparar item: {e}")
        
        if not prepared:
            print("  ⚠️ Nenhum item válido para inserir")
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        # Normaliza chaves do batch (remove campos inválidos por tabela)
        prepared = self._normalize_batch_keys(prepared, tabela)
        
        stats = {'inserted': 0, 'updated': 0, 'errors': 0}
        batch_size = 500
        total_batches = (len(prepared) + batch_size - 1) // batch_size
        
        url = f"{self.url}/rest/v1/{tabela}"
        
        for i in range(0, len(prepared), batch_size):
            batch = prepared[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            
            try:
                r = self.session.post(url, json=batch, timeout=120)
                
                if r.status_code in (200, 201):
                    stats['inserted'] += len(batch)
                    print(f"  ✅ Batch {batch_num}/{total_batches}: {len(batch)} itens")
                elif r.status_code == 409:
                    stats['updated'] += len(batch)
                    print(f"  🔄 Batch {batch_num}/{total_batches}: {len(batch)} atualizados")
                else:
                    error_msg = r.text[:200] if r.text else 'Sem detalhes'
                    print(f"  ❌ Batch {batch_num}: HTTP {r.status_code} - {error_msg}")
                    stats['errors'] += len(batch)
            
            except Exception as e:
                print(f"  ❌ Batch {batch_num}: {e}")
                stats['errors'] += len(batch)
            
            if batch_num < total_batches:
                time.sleep(0.5)
        
        return stats
    
    def _normalize_batch_keys(self, items: list, tabela: str = '') -> list:
        """Garante que todos os itens tenham apenas campos válidos para a tabela"""
        if not items:
            return items
        
        # Campos padrão presentes em TODAS as tabelas
        standard_fields = {
            'source', 'external_id', 'title', 'normalized_title', 'description_preview',
            'description', 'value', 'value_text', 'city', 'state', 'address',
            'auction_date', 'days_remaining', 'auction_type', 'auction_name',
            'store_name', 'lot_number', 'total_visits', 'total_bids', 'total_bidders',
            'link', 'metadata', 'duplicate_group', 'is_primary_duplicate',
            'is_active', 'created_at', 'updated_at', 'last_scraped_at',
            'market_price', 'market_price_source', 'market_price_updated_at',
            'market_price_confidence', 'market_price_metadata',
            # ✅ Novos campos de praça
            'auction_round', 'discount_percentage', 'first_round_value', 'first_round_date'
        }
        
        # Campos específicos por tabela
        table_specific_fields = {
            'veiculos': {'vehicle_type'},
            'imoveis': {'property_type'},
            'animais': {'animal_type'},
            'tecnologia': {'multiplecategory', 'tech_type'},
            'bens_consumo': {'consumption_goods_type'},
            'partes_pecas': {'parts_type'},
            'nichados': {'specialized_type'},
            'eletrodomesticos': {'appliance_type'},
            'materiais_construcao': {'construction_material_type'},
        }
        
        # Campos permitidos para esta tabela
        allowed_fields = standard_fields.copy()
        if tabela in table_specific_fields:
            allowed_fields.update(table_specific_fields[tabela])
        
        # Coleta todas as chaves válidas do batch
        all_keys = set()
        for item in items:
            for key in item.keys():
                if key in allowed_fields:
                    all_keys.add(key)
        
        # Normaliza cada item para ter todas as chaves válidas
        normalized = []
        for item in items:
            normalized_item = {}
            for key in all_keys:
                normalized_item[key] = item.get(key, None)
            normalized.append(normalized_item)
        
        return normalized
    
    def _prepare(self, item: dict, tabela: str = '') -> dict:
        """Prepara item para inserção no banco"""
        source = item.get('source')
        external_id = item.get('external_id')
        title = item.get('title') or 'Sem Título'
        
        if not source or not external_id:
            return None
        
        # Processa auction_date
        auction_date = item.get('auction_date')
        if auction_date and isinstance(auction_date, str):
            try:
                auction_date = auction_date.replace('Z', '+00:00')
                dt = datetime.fromisoformat(auction_date)
                auction_date = dt.isoformat()
            except:
                auction_date = None
        
        # Valida state
        state = item.get('state')
        if state:
            state = str(state).strip().upper()
            if len(state) != 2:
                state = None
        
        # Processa value
        value = item.get('value')
        if value is not None:
            try:
                value = float(value)
                if value < 0:
                    value = None
            except:
                value = None
        
        metadata = item.get('metadata', {})
        if not isinstance(metadata, dict):
            metadata = {}
        
        # ✅ Campos padrão (presentes em TODAS as tabelas)
        data = {
            'source': str(source),
            'external_id': str(external_id),
            'title': str(title)[:255],
            'normalized_title': str(item.get('normalized_title') or title)[:255],
            'description_preview': str(item.get('description_preview', ''))[:255] if item.get('description_preview') else None,
            'description': str(item.get('description')) if item.get('description') else None,
            'value': value,
            'value_text': str(item.get('value_text')) if item.get('value_text') else None,
            'city': str(item.get('city')) if item.get('city') else None,
            'state': state,
            'address': str(item.get('address')) if item.get('address') else None,
            'auction_date': auction_date,
            'days_remaining': int(item.get('days_remaining', 0)) if item.get('days_remaining') is not None else None,
            'auction_type': str(item.get('auction_type', 'Leilão'))[:100],
            'auction_name': str(item.get('auction_name')) if item.get('auction_name') else None,
            'store_name': str(item.get('store_name')) if item.get('store_name') else None,
            'lot_number': str(item.get('lot_number')) if item.get('lot_number') else None,
            'total_visits': int(item.get('total_visits', 0)),
            'total_bids': int(item.get('total_bids', 0)),
            'total_bidders': int(item.get('total_bidders', 0)),
            'link': str(item.get('link')) if item.get('link') else None,
            'metadata': metadata,
            'is_active': True,
            'last_scraped_at': datetime.now().isoformat(),
            # ✅ Informações de praça
            'auction_round': int(item.get('auction_round')) if item.get('auction_round') is not None else None,
            'discount_percentage': float(item.get('discount_percentage')) if item.get('discount_percentage') is not None else None,
            'first_round_value': float(item.get('first_round_value')) if item.get('first_round_value') is not None else None,
            'first_round_date': str(item.get('first_round_date')) if item.get('first_round_date') else None,
        }
        
        # ✅ Campos específicos por tabela
        if tabela == 'veiculos':
            # Tenta pegar do root primeiro, depois do metadata
            vehicle_type = item.get('vehicle_type')
            if not vehicle_type and isinstance(metadata, dict):
                vehicle_type = metadata.get('vehicle_type')
            
            if vehicle_type:
                data['vehicle_type'] = str(vehicle_type)[:255]
        
        if tabela == 'imoveis':
            # Property type para imóveis
            property_type = item.get('property_type')
            if not property_type and isinstance(metadata, dict):
                property_type = metadata.get('property_type')
            
            if property_type:
                data['property_type'] = str(property_type)[:255]
        
        if tabela == 'animais':
            # Animal type para animais
            animal_type = item.get('animal_type')
            if not animal_type and isinstance(metadata, dict):
                animal_type = metadata.get('animal_type')
            
            if animal_type:
                data['animal_type'] = str(animal_type)[:255]
        
        if tabela == 'tecnologia':
            # Multiple category para tecnologia
            multiplecategory = item.get('multiplecategory')
            if not multiplecategory and isinstance(metadata, dict):
                multiplecategory = metadata.get('multiplecategory')
            
            if multiplecategory and isinstance(multiplecategory, list):
                data['multiplecategory'] = multiplecategory
            
            # Tech type (novo campo adicional)
            tech_type = item.get('tech_type')
            if not tech_type and isinstance(metadata, dict):
                tech_type = metadata.get('tech_type')
            
            if tech_type:
                data['tech_type'] = str(tech_type)[:255]
        
        if tabela == 'bens_consumo':
            # Consumption goods type
            consumption_goods_type = item.get('consumption_goods_type')
            if not consumption_goods_type and isinstance(metadata, dict):
                consumption_goods_type = metadata.get('consumption_goods_type')
            
            if consumption_goods_type:
                data['consumption_goods_type'] = str(consumption_goods_type)[:255]
        
        if tabela == 'partes_pecas':
            # Parts type
            parts_type = item.get('parts_type')
            if not parts_type and isinstance(metadata, dict):
                parts_type = metadata.get('parts_type')
            
            if parts_type:
                data['parts_type'] = str(parts_type)[:255]
        
        if tabela == 'nichados':
            # Specialized type
            specialized_type = item.get('specialized_type')
            if not specialized_type and isinstance(metadata, dict):
                specialized_type = metadata.get('specialized_type')
            
            if specialized_type:
                data['specialized_type'] = str(specialized_type)[:255]
        
        if tabela == 'eletrodomesticos':
            # Appliance type
            appliance_type = item.get('appliance_type')
            if not appliance_type and isinstance(metadata, dict):
                appliance_type = metadata.get('appliance_type')
            
            if appliance_type:
                data['appliance_type'] = str(appliance_type)[:255]
        
        if tabela == 'materiais_construcao':
            # Construction material type
            construction_material_type = item.get('construction_material_type')
            if not construction_material_type and isinstance(metadata, dict):
                construction_material_type = metadata.get('construction_material_type')
            
            if construction_material_type:
                data['construction_material_type'] = str(construction_material_type)[:255]
        
        return data
    
    def test(self) -> bool:
        """Testa conexão"""
        try:
            url = f"{self.url}/rest/v1/"
            r = self.session.get(url, timeout=10)
            
            if r.status_code == 200:
                print("✅ Conexão com Supabase OK")
                return True
            else:
                print(f"❌ Erro HTTP {r.status_code}")
                return False
        except Exception as e:
            print(f"❌ Erro: {e}")
            return False
    
    def get_stats(self, tabela: str) -> dict:
        """Retorna estatísticas"""
        try:
            url = f"{self.url}/rest/v1/{tabela}"
            r = self.session.get(
                url,
                params={'select': 'count'},
                headers={**self.headers, 'Prefer': 'count=exact'},
                timeout=30
            )
            
            if r.status_code == 200:
                total = int(r.headers.get('Content-Range', '0').split('/')[-1])
                return {'total': total, 'table': tabela}
        except:
            pass
        
        return {'total': 0, 'table': tabela}
    
    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()