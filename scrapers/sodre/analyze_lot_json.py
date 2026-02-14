#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ANÁLISE DO JSON DO LOTE
Analisa os dados do lote que você já forneceu para ver o problema

Uso: python3 analyze_lot_json.py
"""

import json

# JSON que você forneceu
lot_exemplo_1 = {
    "idx": 9,
    "id": 81577,
    "external_id": "sodre_12840014",
    "lot_id": 12840014,
    "lot_number": "001",
    "lot_inspection_number": "128400",
    "lot_inspection_id": 385103,
    "auction_id": 28119,
    "category": "esquadrias e estruturas metálicas",
    "title": "portão de ferro de correr",
    "link": "https://leilao.sodresantoro.com.br/leilao/28119/lote/12840014/"
}

# Outro exemplo que você mencionou (o problema)
lot_exemplo_2 = {
    "source": "sodre",
    "external_id": "sodre_2714738",
    "title": "Lotes em Leilão",
    "description": "VOLKSWAGEN 8.150E DELIVERY 07/08",
    "lot_number": "0030",
    "link": "https://leilao.sodresantoro.com.br/leilao/28040/lote/2714738/",
    "metadata": {
        "leilao_id": "28040",
        "codigo_interno": "954584"
    }
}


def analyze_lot(lot_data, lot_name):
    print("\n" + "="*70)
    print(f"📋 ANALISANDO: {lot_name}")
    print("="*70)
    
    print("\n📄 JSON COMPLETO:")
    print(json.dumps(lot_data, indent=2, ensure_ascii=False))
    
    print("\n🔍 CAMPOS IMPORTANTES:")
    print("-"*70)
    
    # IDs disponíveis
    id_fields = {
        'id': lot_data.get('id'),
        'lot_id': lot_data.get('lot_id'),
        'external_id': lot_data.get('external_id'),
        'lot_inspection_id': lot_data.get('lot_inspection_id'),
    }
    
    for field, value in id_fields.items():
        if value:
            print(f"  {field:25} = {value}")
    
    # Link
    link = lot_data.get('link')
    print(f"\n🔗 LINK:")
    print(f"  {link}")
    
    # Extrai ID do link
    if link and '/lote/' in link:
        parts = link.split('/lote/')
        link_id = parts[1].rstrip('/')
        
        print(f"\n🎯 ID NO LINK: {link_id}")
        
        print(f"\n📊 COMPARAÇÃO COM CAMPOS:")
        print("-"*70)
        
        for field, value in id_fields.items():
            if value:
                matches = str(value) == link_id
                symbol = "✅" if matches else "❌"
                print(f"  {symbol} {field:20} = {value:15} {'(MATCH!)' if matches else ''}")
        
        # Extrai auction_id do link também
        if '/leilao/' in link:
            auction_part = link.split('/leilao/')[1].split('/')[0]
            print(f"\n🎪 auction_id no link: {auction_part}")
            if lot_data.get('auction_id'):
                print(f"   auction_id no JSON: {lot_data.get('auction_id')}")
    
    print()


def main():
    print("\n" + "="*80)
    print("🔍 ANÁLISE DE LOTES DO SODRÉ - DEBUG DE LINKS")
    print("="*80)
    
    analyze_lot(lot_exemplo_1, "EXEMPLO 1 - Portão de ferro")
    analyze_lot(lot_exemplo_2, "EXEMPLO 2 - Volkswagen")
    
    print("\n" + "="*80)
    print("💡 CONCLUSÕES:")
    print("="*80)
    print("""
1. A API DO SODRÉ JÁ RETORNA O CAMPO 'link' PRONTO! ✅
   
2. O 'lot_id' que vem da API PODE ser diferente do ID usado no link público
   
3. NÃO devemos construir o link manualmente usando lot_id
   
4. SOLUÇÃO: Sempre usar lot.get('link') da API

EXEMPLO CÓDIGO CORRETO:
    'link': lot.get('link')  # ← USA O LINK DA API
    
EXEMPLO CÓDIGO ERRADO:
    'link': f"https://leilao.sodresantoro.com.br/leilao/{auction_id}/lote/{lot_id}/"
    """)
    
    print("\n" + "="*80)
    print("🔧 AÇÃO NECESSÁRIA:")
    print("="*80)
    print("""
1. Use o scraper.py atualizado (já corrigido)
2. O campo 'link' agora vem direto da API
3. Execute: python3 scraper.py
4. Verifique se os links estão corretos no banco
    """)
    
    print("="*80)


if __name__ == "__main__":
    main()