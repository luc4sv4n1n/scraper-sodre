#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPABASE CLIENT - VERSÃO CORRIGIDA
✅ Normaliza chaves antes de enviar (fix PGRST102)
✅ Todos os items no batch têm as mesmas chaves
"""

import os
import time
import requests
from datetime import datetime
from typing import List, Dict, Optional


class SupabaseClient:
    """Cliente para Supabase - Com normalização de chaves"""
    
    def __init__(self, service_name: str = None, service_type: str = 'scraper'):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("⚠ Configure SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY")
        
        self.url = self.url.rstrip('/')
        
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Content-Profile': 'auctions',
            'Accept-Profile': 'auctions',
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Heartbeat
        self.service_name = service_name
        self.service_type = service_type
        self.heartbeat_id = None
        self.heartbeat_enabled = bool(service_name)
        self.start_time = time.time()
        self.items_processed = 0
    
    # ========================================================================
    # MÉTODOS HEARTBEAT (COPIADOS DO ORIGINAL)
    # ========================================================================
    
    def heartbeat_start(self, metadata: Optional[Dict] = None) -> bool:
        if not self.heartbeat_enabled:
            return False
        
        try:
            url = f"{self.url}/rest/v1/infra_actions"
            
            payload = {
                'service_name': self.service_name,
                'service_type': self.service_type,
                'status': 'active',
                'last_activity': datetime.now().isoformat(),
                'logs': {
                    'started_at': datetime.now().isoformat(),
                    'items_processed': 0,
                },
                'metadata': metadata or {},
            }
            
            heartbeat_headers = {
                **self.headers,
                'Content-Profile': 'public',
                'Accept-Profile': 'public',
                'Prefer': 'return=representation'
            }
            
            r = self.session.post(url, json=payload, headers=heartbeat_headers, timeout=10)
            
            if r.status_code in (200, 201):
                data = r.json()
                if data and len(data) > 0:
                    self.heartbeat_id = data[0].get('id')
                    print(f"  💓 Heartbeat iniciado: {self.heartbeat_id}")
                    return True
        
        except Exception as e:
            print(f"  ⚠️ Erro ao iniciar heartbeat: {e}")
        
        return False
    
    def heartbeat_update(self, status: str = 'active', custom_logs: Optional[Dict] = None, error_message: Optional[str] = None) -> bool:
        if not self.heartbeat_enabled or not self.heartbeat_id:
            return False
        
        try:
            url = f"{self.url}/rest/v1/infra_actions"
            params = {'id': f'eq.{self.heartbeat_id}'}
            
            elapsed = time.time() - self.start_time
            
            logs = {
                'last_update': datetime.now().isoformat(),
                'items_processed': self.items_processed,
                'elapsed_seconds': round(elapsed, 2),
            }
            
            if custom_logs:
                logs.update(custom_logs)
            
            payload = {
                'status': status,
                'last_activity': datetime.now().isoformat(),
                'logs': logs,
            }
            
            if error_message:
                payload['error_message'] = error_message
            
            heartbeat_headers = {
                **self.headers,
                'Content-Profile': 'public',
                'Accept-Profile': 'public',
            }
            
            r = self.session.patch(url, params=params, json=payload, headers=heartbeat_headers, timeout=10)
            
            return r.status_code == 204
        
        except:
            return False
    
    def heartbeat_progress(self, items_processed: int = 0, custom_logs: Optional[Dict] = None) -> bool:
        self.items_processed += items_processed
        return self.heartbeat_update(status='active', custom_logs=custom_logs)
    
    def heartbeat_finish(self, status: str = 'inactive', final_stats: Optional[Dict] = None) -> bool:
        if not self.heartbeat_enabled or not self.heartbeat_id:
            return False
        
        custom_logs = {
            'finished_at': datetime.now().isoformat(),
            'total_items_processed': self.items_processed,
            'total_elapsed_seconds': round(time.time() - self.start_time, 2),
        }
        
        if final_stats:
            custom_logs.update(final_stats)
        
        success = self.heartbeat_update(status=status, custom_logs=custom_logs)
        
        if success:
            print(f"  💓 Heartbeat finalizado: {self.items_processed} itens")
        
        return success
    
    def heartbeat_error(self, error_message: str) -> bool:
        return self.heartbeat_update(status='error', error_message=error_message)
    
    # ========================================================================
    # NOVO: NORMALIZAÇÃO DE CHAVES
    # ========================================================================
    
    def _normalize_batch_keys(self, items: List[Dict]) -> List[Dict]:
        """
        Normaliza todos os items para terem as mesmas chaves
        ✅ Resolve PGRST102: "All object keys must match"
        """
        if not items:
            return items
        
        # Coleta todas as chaves únicas de todos os items
        all_keys = set()
        for item in items:
            all_keys.update(item.keys())
        
        # Normaliza cada item para ter todas as chaves
        normalized = []
        for item in items:
            normalized_item = {}
            for key in all_keys:
                normalized_item[key] = item.get(key, None)
            normalized.append(normalized_item)
        
        return normalized
    
    # ========================================================================
    # UPSERT CORRIGIDO
    # ========================================================================
    
    def upsert(self, tabela: str, items: List[Dict]) -> Dict:
        """
        Upsert com normalização de chaves
        ✅ Fix PGRST102: Normaliza chaves antes de enviar
        """
        if not items:
            return {'inserted': 0, 'updated': 0, 'errors': 0, 'total': 0}
        
        # Prepara timestamps
        now = datetime.now().isoformat()
        for item in items:
            item['last_scraped_at'] = now
            if 'updated_at' not in item or not item['updated_at']:
                item['updated_at'] = now
            if 'created_at' in item:
                del item['created_at']
        
        stats = {'inserted': 0, 'updated': 0, 'errors': 0, 'total': len(items)}
        batch_size = 500
        total_batches = (len(items) + batch_size - 1) // batch_size
        
        url = f"{self.url}/rest/v1/{tabela}?on_conflict=external_id"
        
        upsert_headers = {
            **self.headers,
            'Prefer': 'resolution=merge-duplicates,return=representation'
        }
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            
            try:
                # ✅ NORMALIZA CHAVES DO BATCH
                normalized_batch = self._normalize_batch_keys(batch)
                
                # Envia batch normalizado
                r = self.session.post(
                    url,
                    json=normalized_batch,
                    headers=upsert_headers,
                    timeout=120
                )
                
                if r.status_code in (200, 201):
                    try:
                        response_data = r.json()
                        if isinstance(response_data, list):
                            stats['inserted'] += len(response_data)
                        else:
                            stats['inserted'] += len(batch)
                    except:
                        stats['inserted'] += len(batch)
                    
                    print(f"  ✅ Batch {batch_num}/{total_batches}: {len(batch)} itens processados")
                    
                    self.heartbeat_progress(
                        items_processed=len(batch),
                        custom_logs={'batch': batch_num, 'total_batches': total_batches}
                    )
                
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
            
            if batch_num < total_batches:
                time.sleep(0.5)
        
        return stats
    
    # ========================================================================
    # MÉTODOS AUXILIARES (COPIADOS DO ORIGINAL)
    # ========================================================================
    
    def test(self) -> bool:
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
        try:
            url = f"{self.url}/rest/v1/{tabela}"
            
            r = self.session.get(
                url,
                params={'select': 'count'},
                headers={**self.headers, 'Prefer': 'count=exact'},
                timeout=30
            )
            
            if r.status_code == 200:
                total = int(r.headers.get('Content-Range', '0/0').split('/')[-1])
                
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
    
    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()