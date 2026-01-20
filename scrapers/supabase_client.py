#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPABASE CLIENT - SODRÉ ITEMS (CORRIGIDO)
✅ Upsert correto com on_conflict
✅ Prefer header adequado
✅ Tratamento de erros melhorado
"""

import os
import time
import requests
from datetime import datetime
from typing import List, Dict


class SupabaseClient:
    """Cliente para Supabase - Tabela sodre_items completa"""
    
    def __init__(self):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("❌ Configure SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY")
        
        self.url = self.url.rstrip('/')
        
        # ✅ Headers base (sem Prefer - será adicionado por operação)
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Content-Profile': 'auctions',
            'Accept-Profile': 'auctions',
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def upsert(self, tabela: str, items: List[Dict]) -> Dict:
        """
        Upsert correto na tabela sodre_items
        ✅ Usa external_id como chave de conflito
        ✅ Atualiza registros existentes
        ✅ Insere novos registros
        """
        if not items:
            return {'inserted': 0, 'updated': 0, 'errors': 0, 'total': 0}
        
        # Prepara items com timestamps
        now = datetime.now().isoformat()
        for item in items:
            item['last_scraped_at'] = now
            if 'updated_at' not in item or not item['updated_at']:
                item['updated_at'] = now
            # created_at só é definido no banco via DEFAULT now()
            # não enviamos para permitir que novos registros usem o default
            if 'created_at' in item:
                del item['created_at']
        
        stats = {'inserted': 0, 'updated': 0, 'errors': 0, 'total': len(items)}
        batch_size = 500
        total_batches = (len(items) + batch_size - 1) // batch_size
        
        # ✅ URL com on_conflict para upsert correto
        url = f"{self.url}/rest/v1/{tabela}?on_conflict=external_id"
        
        # ✅ Headers específicos para upsert
        upsert_headers = {
            **self.headers,
            'Prefer': 'resolution=merge-duplicates,return=representation'
        }
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            
            try:
                # ✅ POST com on_conflict = UPSERT
                r = self.session.post(
                    url,
                    json=batch,
                    headers=upsert_headers,
                    timeout=120
                )
                
                if r.status_code in (200, 201):
                    # Tenta contar inserted vs updated pela resposta
                    try:
                        response_data = r.json()
                        if isinstance(response_data, list):
                            # Se retornou dados, conta como sucesso
                            stats['inserted'] += len(response_data)
                        else:
                            # Se não retornou, assume todos inseridos/atualizados
                            stats['inserted'] += len(batch)
                    except:
                        # Se não conseguiu parsear, assume sucesso
                        stats['inserted'] += len(batch)
                    
                    print(f"  ✅ Batch {batch_num}/{total_batches}: {len(batch)} itens processados")
                
                else:
                    error_msg = r.text[:300] if r.text else 'Sem detalhes'
                    print(f"  ❌ Batch {batch_num}/{total_batches}: HTTP {r.status_code}")
                    print(f"     Erro: {error_msg}")
                    stats['errors'] += len(batch)
            
            except requests.exceptions.Timeout:
                print(f"  ⏱️ Batch {batch_num}/{total_batches}: Timeout (120s)")
                stats['errors'] += len(batch)
            
            except Exception as e:
                print(f"  ❌ Batch {batch_num}/{total_batches}: {type(e).__name__}: {str(e)[:200]}")
                stats['errors'] += len(batch)
            
            # Pausa entre batches (exceto no último)
            if batch_num < total_batches:
                time.sleep(0.5)
        
        return stats
    
    def get_upsert_stats_detailed(self, tabela: str, items: List[Dict]) -> Dict:
        """
        Versão alternativa que verifica quais items já existem
        para dar estatísticas precisas de inserted vs updated
        """
        if not items:
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        # Busca external_ids existentes
        external_ids = [item['external_id'] for item in items if 'external_id' in item]
        
        if not external_ids:
            return {'inserted': 0, 'updated': 0, 'errors': len(items)}
        
        try:
            # Consulta quais já existem
            url = f"{self.url}/rest/v1/{tabela}"
            params = {
                'select': 'external_id',
                'external_id': f"in.({','.join(external_ids)})"
            }
            
            r = self.session.get(url, params=params, timeout=30)
            
            if r.status_code == 200:
                existing_ids = {item['external_id'] for item in r.json()}
                
                new_items = [item for item in items if item['external_id'] not in existing_ids]
                update_items = [item for item in items if item['external_id'] in existing_ids]
                
                return {
                    'existing_count': len(existing_ids),
                    'new_count': len(new_items),
                    'update_count': len(update_items)
                }
        except Exception as e:
            print(f"  ⚠️ Erro ao verificar existentes: {e}")
        
        return {'existing_count': 0, 'new_count': 0, 'update_count': 0}
    
    def test(self) -> bool:
        """Testa conexão"""
        try:
            url = f"{self.url}/rest/v1/"
            r = self.session.get(url, timeout=10)
            
            if r.status_code == 200:
                print("✅ Conexão com Supabase OK")
                return True
            else:
                print(f"❌ Erro HTTP {r.status_code}: {r.text[:200]}")
                return False
        except Exception as e:
            print(f"❌ Erro: {e}")
            return False
    
    def get_stats(self, tabela: str) -> Dict:
        """Retorna estatísticas da tabela"""
        try:
            url = f"{self.url}/rest/v1/{tabela}"
            
            # Conta total
            r = self.session.get(
                url,
                params={'select': 'count'},
                headers={**self.headers, 'Prefer': 'count=exact'},
                timeout=30
            )
            
            if r.status_code == 200:
                total = int(r.headers.get('Content-Range', '0/0').split('/')[-1])
                
                # Conta ativos
                r_active = self.session.get(
                    url,
                    params={'select': 'count', 'is_active': 'eq.true'},
                    headers={**self.headers, 'Prefer': 'count=exact'},
                    timeout=30
                )
                
                active = 0
                if r_active.status_code == 200:
                    active = int(r_active.headers.get('Content-Range', '0/0').split('/')[-1])
                
                return {
                    'total': total,
                    'active': active,
                    'inactive': total - active,
                    'table': tabela
                }
        except Exception as e:
            print(f"  ⚠️ Erro ao buscar stats: {e}")
        
        return {'total': 0, 'active': 0, 'inactive': 0, 'table': tabela}
    
    def verify_upsert(self, tabela: str, sample_external_ids: List[str]) -> Dict:
        """
        Verifica se alguns external_ids específicos existem no banco
        Útil para debug
        """
        if not sample_external_ids:
            return {}
        
        try:
            url = f"{self.url}/rest/v1/{tabela}"
            params = {
                'select': 'external_id,title,updated_at,last_scraped_at',
                'external_id': f"in.({','.join(sample_external_ids[:5])})"  # Máx 5
            }
            
            r = self.session.get(url, params=params, timeout=30)
            
            if r.status_code == 200:
                results = r.json()
                print(f"\n🔍 Verificação de {len(results)} registros:")
                for item in results:
                    print(f"  • {item['external_id']}: {item['title'][:50]}...")
                    print(f"    Updated: {item.get('updated_at', 'N/A')}")
                    print(f"    Scraped: {item.get('last_scraped_at', 'N/A')}")
                
                return {'found': len(results), 'samples': results}
        except Exception as e:
            print(f"  ⚠️ Erro na verificação: {e}")
        
        return {'found': 0, 'samples': []}
    
    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()