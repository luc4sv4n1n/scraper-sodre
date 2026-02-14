#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VALIDADOR DE LOTES SODRÉ - VERSÃO SIMPLIFICADA
Coloque este arquivo na mesma pasta do scraper.py

Uso:
    python3 validate_sodre_lots.py

O que faz:
- Busca lotes do banco com auction_status='aberto'
- Verifica se redirecionam para 'lotes-encerrados'
- Marca como is_active=false no banco
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict
from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from supabase_client_fixed import SupabaseClient
except:
    try:
        from supabase_client import SupabaseClient
    except:
        SupabaseClient = None


async def check_lot(lot_link: str = None, auction_id: int = None, lot_id: int = None, debug: bool = False) -> str:
    """
    Verifica se lote está ativo ou encerrado acessando a página
    
    Args:
        lot_link: Link direto do lote (preferencial, vem do banco)
        auction_id: ID do leilão (fallback)
        lot_id: ID do lote (fallback)
        debug: Modo debug
    
    Retorna: 'ativo', 'encerrado' ou 'erro'
    """
    # Usa o link do banco se disponível, senão constrói
    if lot_link:
        url = lot_link
    elif auction_id and lot_id:
        url = f"https://leilao.sodresantoro.com.br/leilao/{auction_id}/lote/{lot_id}/"
    else:
        return 'erro'
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(2)
            
            final_url = page.url
            
            if "lotes-encerrados" in final_url:
                if debug:
                    print(f"    ❌ Lote {lot_id} → encerrado")
                return 'encerrado'
            else:
                if debug:
                    print(f"    ✅ Lote {lot_id} → ativo")
                return 'ativo'
                
        except Exception as e:
            if debug:
                print(f"    ⚠️ Erro lote {lot_id}: {e}")
            return 'erro'
        finally:
            await browser.close()


async def validate_batch(lots: List[Dict], batch_size: int = 5, debug: bool = False):
    """Valida lotes em batches paralelos"""
    stats = {'checked': 0, 'encerrados': 0, 'updated': 0, 'errors': 0}
    
    for i in range(0, len(lots), batch_size):
        batch = lots[i:i+batch_size]
        
        if not debug:
            print(f"  📦 Validando lotes {i+1}-{min(i+batch_size, len(lots))} de {len(lots)}...")
        
        tasks = [
            check_lot(
                lot_link=lot.get('link'),
                auction_id=lot.get('auction_id'), 
                lot_id=lot.get('lot_id'),
                debug=debug
            )
            for lot in batch
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for lot, result in zip(batch, results):
            if result == 'encerrado':
                stats['encerrados'] += 1
                lot['needs_update'] = True
            stats['checked'] += 1
        
        if i + batch_size < len(lots):
            await asyncio.sleep(2)
    
    return stats


async def main():
    print("\n" + "="*70)
    print("🔍 VALIDADOR DE LOTES SODRÉ SANTORO")
    print("="*70)
    
    if not SupabaseClient:
        print("❌ SupabaseClient não disponível")
        return
    
    supabase = SupabaseClient(service_name='sodre_validator', service_type='validator')
    
    if not supabase.test():
        print("❌ Erro ao conectar com Supabase")
        return
    
    print("\n📥 Buscando lotes do banco...")
    
    # Busca lotes com auction_status='aberto'
    try:
        response = supabase.client.table('sodre_items').select(
            'lot_id,auction_id,external_id,title,link'
        ).eq('source', 'sodre').eq('is_active', True).eq(
            'auction_status', 'aberto'
        ).order('updated_at', desc=True).limit(100).execute()
        
        lots = response.data if response.data else []
    except Exception as e:
        print(f"❌ Erro ao buscar lotes: {e}")
        return
    
    if not lots:
        print("✅ Nenhum lote encontrado com auction_status='aberto'")
        return
    
    print(f"✅ {len(lots)} lotes encontrados")
    
    print("\n🔍 Validando lotes...")
    stats = await validate_batch(lots, batch_size=5, debug=False)
    
    # Filtra lotes a desativar
    lots_to_update = [lot for lot in lots if lot.get('needs_update')]
    
    if lots_to_update:
        print(f"\n❌ {len(lots_to_update)} lotes encerrados detectados")
        print("\n🔄 Atualizando banco...")
        
        for lot in lots_to_update:
            try:
                supabase.client.table('sodre_items').update({
                    'is_active': False,
                    'auction_status': 'Encerrado',
                    'updated_at': datetime.now().isoformat(),
                }).eq('lot_id', lot['lot_id']).eq('auction_id', lot['auction_id']).execute()
                
                stats['updated'] += 1
            except Exception as e:
                print(f"  ⚠️ Erro ao atualizar lote {lot['lot_id']}: {e}")
                stats['errors'] += 1
        
        print(f"✅ {stats['updated']} lotes desativados")
    else:
        print("\n✅ Nenhum lote encerrado detectado")
    
    print("\n" + "="*70)
    print("📊 ESTATÍSTICAS")
    print("="*70)
    print(f"  • Verificados: {stats['checked']}")
    print(f"  • Encerrados: {stats['encerrados']}")
    print(f"  • Atualizados: {stats['updated']}")
    print(f"  • Erros: {stats['errors']}")
    print(f"\n✅ Concluído: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    asyncio.run(main())