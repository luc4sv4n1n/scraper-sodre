#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPABASE CLIENT - SODRÉ ITEMS (COM HEARTBEAT)
✅ Upsert correto com on_conflict
✅ Prefer header adequado
✅ Tratamento de erros melhorado
✅ Sistema de heartbeat para monitoramento
"""

import os
import time
import requests
from datetime import datetime
from typing import List, Dict, Optional


class SupabaseClient:
    """Cliente para Supabase - Tabela sodre_items completa + Heartbeat"""
    
    def __init__(self, service_name: str = None, service_type: str = 'scraper'):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("⚠ Configure SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY")
        
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
        
        # ✅ HEARTBEAT - Atributos de controle
        self.service_name = service_name
        self.service_type = service_type
        self.heartbeat_id = None
        self.heartbeat_enabled = bool(service_name)  # Só ativa se receber service_name
        self.start_time = time.time()
        self.items_processed = 0
    
    # ========================================================================
    # MÉTODOS HEARTBEAT (NOVOS - NÃO QUEBRAM NADA)
    # ========================================================================
    
    def heartbeat_start(self, metadata: Optional[Dict] = None) -> bool:
        """
        Inicia heartbeat no infra_actions
        ✅ Seguro: Se falhar, só loga mas não interrompe
        """
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
            
            # ✅ Headers para schema PUBLIC (infra_actions está em public)
            heartbeat_headers = {
                **self.headers,
                'Content-Profile': 'public',
                'Accept-Profile': 'public',
                'Prefer': 'return=representation'
            }
            
            r = self.session.post(
                url,
                json=payload,
                headers=heartbeat_headers,
                timeout=10
            )
            
            if r.status_code in (200, 201):
                data = r.json()
                if data and len(data) > 0:
                    self.heartbeat_id = data[0].get('id')
                    print(f"  💓 Heartbeat iniciado: {self.heartbeat_id}")
                    return True
        
        except Exception as e:
            print(f"  ⚠️ Erro ao iniciar heartbeat: {e}")
        
        return False
    
    def heartbeat_update(
        self,
        status: str = 'active',
        custom_logs: Optional[Dict] = None,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Atualiza heartbeat
        ✅ Seguro: Se falhar, não quebra o scraper
        """
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
            
            # ✅ Headers para schema PUBLIC
            heartbeat_headers = {
                **self.headers,
                'Content-Profile': 'public',
                'Accept-Profile': 'public',
            }
            
            r = self.session.patch(
                url,
                params=params,
                json=payload,
                headers=heartbeat_headers,
                timeout=10
            )
            
            return r.status_code == 204
        
        except Exception as e:
            # Silencioso - não queremos interromper o scraper por erro de heartbeat
            return False
    
    def heartbeat_progress(
        self,
        items_processed: int = 0,
        custom_logs: Optional[Dict] = None
    ) -> bool:
        """
        Atualiza progresso (chamado durante batches)
        ✅ Seguro: Incrementa contador e atualiza
        """
        self.items_processed += items_processed
        return self.heartbeat_update(status='active', custom_logs=custom_logs)
    
    def heartbeat_finish(
        self,
        status: str = 'inactive',
        final_stats: Optional[Dict] = None
    ) -> bool:
        """
        Finaliza heartbeat
        ✅ Seguro: Marca como concluído
        """
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
            print(f"  💓 Heartbeat finalizado: {self.items_processed} itens em {custom_logs['total_elapsed_seconds']}s")
        
        return success
    
    def heartbeat_error(self, error_message: str) -> bool:
        """
        Marca heartbeat como erro
        ✅ Seguro: Registra erro mas não interrompe
        """
        return self.heartbeat_update(status='error', error_message=error_message)
    
    # ========================================================================
    # MÉTODOS ORIGINAIS (INALTERADOS, EXCETO UMA LINHA NO UPSERT)
    # ========================================================================
    
    def upsert(self, tabela: str, items: List[Dict]) -> Dict:
        """
        Upsert correto na tabela sodre_items
        ✅ Usa external_id como chave de conflito
        ✅ Atualiza registros existentes
        ✅ Insere novos registros
        ✅ Heartbeat opcional (não quebra nada)
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
                    
                    # ✅ ÚNICA LINHA ADICIONADA - Heartbeat opcional
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