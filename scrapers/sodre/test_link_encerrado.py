#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TESTA SE UM LINK ESTÁ ENCERRADO
Acessa o link e verifica se redireciona para lotes-encerrados

INSTALAÇÃO:
    pip install playwright
    playwright install chromium

USO:
    python3 test_link_encerrado.py
"""

import asyncio
from playwright.async_api import async_playwright


async def check_link(url):
    """Verifica se um link está encerrado"""
    
    print("\n" + "="*70)
    print("🔍 VERIFICANDO LINK")
    print("="*70)
    print(f"\n📡 URL: {url}")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Deixa visível para você ver
        page = await browser.new_page()
        
        try:
            print("\n⏳ Acessando página...")
            response = await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            
            # Aguarda redirects
            await asyncio.sleep(3)
            
            final_url = page.url
            status_code = response.status
            
            print(f"\n✅ Status HTTP: {status_code}")
            print(f"🔗 URL Final: {final_url}")
            
            # Verifica se mudou
            if final_url != url:
                print(f"\n⚠️ REDIRECIONAMENTO DETECTADO!")
                print(f"  De:   {url}")
                print(f"  Para: {final_url}")
            
            # Verifica se tem "lotes-encerrados" na URL
            print("\n" + "="*70)
            print("📊 RESULTADO")
            print("="*70)
            
            if "lotes-encerrados" in final_url:
                print("❌ LOTE ENCERRADO!")
                print("   → Redirecionou para página de lotes encerrados")
                print("   → Este lote NÃO deveria estar no banco com is_active=true")
                return "encerrado"
            
            # Verifica texto na página
            await asyncio.sleep(2)
            page_content = await page.content()
            
            if "encerrado" in page_content.lower():
                print("⚠️ POSSÍVEL LOTE ENCERRADO!")
                print("   → Palavra 'encerrado' encontrada na página")
                
            if "não encontrado" in page_content.lower() or "404" in page_content:
                print("❌ LOTE NÃO ENCONTRADO!")
                print("   → Página retornou erro 404")
                return "nao_encontrado"
            
            # Tenta detectar status na página
            try:
                # Procura por elementos que indiquem status
                status_elem = await page.query_selector('.status, .lote-status, [class*="status"]')
                if status_elem:
                    status_text = await status_elem.inner_text()
                    print(f"\n📋 Status na página: {status_text}")
            except:
                pass
            
            if "lotes-encerrados" not in final_url and status_code == 200:
                print("✅ LOTE APARENTEMENTE ATIVO")
                print("   → Não redirecionou para lotes-encerrados")
                return "ativo"
            else:
                print("⚠️ STATUS INDETERMINADO")
                return "desconhecido"
                
        except Exception as e:
            print(f"\n❌ ERRO: {e}")
            return "erro"
            
        finally:
            print("\n⏸️ Navegador ficará aberto por 10 segundos para você ver...")
            await asyncio.sleep(10)
            await browser.close()


async def main():
    print("\n" + "="*80)
    print("🔍 TESTE DE LINKS - DETECÇÃO DE LOTES ENCERRADOS")
    print("="*80)
    
    # Links para testar
    links_para_testar = [
        # Link do exemplo que você deu (encerrado)
        "https://leilao.sodresantoro.com.br/leilao/28119/lote/12840014/",
        
        # Link do problema que você reportou
        "https://leilao.sodresantoro.com.br/leilao/28132/lote/2727790/",
    ]
    
    resultados = {}
    
    for link in links_para_testar:
        resultado = await check_link(link)
        resultados[link] = resultado
        
        print("\n" + "-"*70)
        input("Pressione ENTER para testar próximo link...")
    
    # Resumo
    print("\n" + "="*80)
    print("📊 RESUMO DOS TESTES")
    print("="*80)
    
    for link, status in resultados.items():
        emoji = "❌" if status == "encerrado" else "✅" if status == "ativo" else "⚠️"
        print(f"\n{emoji} {status.upper()}")
        print(f"   {link}")
    
    print("\n" + "="*80)
    print("💡 CONCLUSÃO:")
    print("="*80)
    
    encerrados = sum(1 for s in resultados.values() if s == "encerrado")
    
    if encerrados > 0:
        print(f"""
❌ {encerrados} lote(s) estão ENCERRADOS!

🔧 PROBLEMA IDENTIFICADO:
   → A API do Sodré retorna lotes com auction_status='aberto'
   → Mas quando acessa o link, o lote está encerrado
   → O scraper está coletando esses lotes indevidamente

✅ SOLUÇÃO JÁ IMPLEMENTADA no scraper.py:
   → Verifica auction_status + lot_status
   → Filtra lotes encerrados antes de salvar
   
🚀 PRÓXIMO PASSO:
   1. Rode o scraper.py atualizado
   2. Rode o validate_sodre_lots.py para limpar banco
   3. Os lotes encerrados não entrarão mais no banco
        """)
    else:
        print("\n✅ Todos os links testados estão ativos!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️ Teste cancelado pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        print("\nVerifique se instalou as dependências:")
        print("  pip install playwright")
        print("  playwright install chromium")