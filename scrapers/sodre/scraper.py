#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SODRÉ SANTORO - SCRAPER COM CATEGORIZAÇÃO REFINADA
✅ Mapeamento completo para 10 categorias principais
✅ Paginação robusta - não para prematuramente
✅ Espera adaptativa por seção
✅ Deduplicação na coleta
✅ Mapeamento completo dos campos
"""

import asyncio
import sys
import json
import time
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


class SodreScraperCategorizado:
    """Scraper Sodré - Com Categorização Refinada"""
    
    def __init__(self, debug=False):
        self.source = 'sodre'
        self.base_url = 'https://www.sodresantoro.com.br'
        self.debug = debug
        
        # ✅ Configuração otimizada por seção
        self.section_config = {
            'veiculos': {'wait_time': 7, 'max_retries': 3, 'max_pages': 200},
            'imoveis': {'wait_time': 7, 'max_retries': 3, 'max_pages': 50},
            'materiais': {'wait_time': 7, 'max_retries': 3, 'max_pages': 200},
            'sucatas': {'wait_time': 12, 'max_retries': 4, 'max_pages': 200},
        }
        
        self.urls = [
            f"{self.base_url}/veiculos/lotes?sort=auction_date_init_asc",
            f"{self.base_url}/imoveis/lotes?sort=auction_date_init_asc",
            f"{self.base_url}/materiais/lotes?sort=auction_date_init_asc",
            f"{self.base_url}/sucatas/lotes?sort=auction_date_init_asc",
        ]
        
        self.stats = {
            'total_scraped': 0,
            'duplicates': 0,
            'with_bids': 0,
            'errors': 0,
        }
        
        self.section_counters = {}
        
        # 🔥 MAPEAMENTO COMPLETO: subcategorias → 10 categorias principais
        self.category_mapping = {
            # ========================================
            # 1️⃣ IMÓVEIS
            # ========================================
            'apartamento': 'Imóveis',
            'casa': 'Imóveis',
            'casa / construção': 'Imóveis',
            'complexo industrial': 'Imóveis',
            'complexo residencial e de lazer': 'Imóveis',
            'direitos sobre apartamento': 'Imóveis',
            'direitos sobre imóvel residencial': 'Imóveis',
            'direitos sobre terreno': 'Imóveis',
            'galpão industrial': 'Imóveis',
            'galpões comerciais e residência': 'Imóveis',
            'gleba de terra': 'Imóveis',
            'imóvel comercial e residencial': 'Imóveis',
            'imóvel residencial': 'Imóveis',
            'imóvel residencial com 3 edificações': 'Imóveis',
            'imóvel residencial tipo sobrado': 'Imóveis',
            'lote de terreno': 'Imóveis',
            'parte ideal de 1/6 sobre imóvel residencial': 'Imóveis',
            'parte ideal de 50% sobre lote de terreno': 'Imóveis',
            'parte ideal de 50% sobre nua-propriedade': 'Imóveis',
            'terreno': 'Imóveis',
            'terreno urbano': 'Imóveis',
            'área de terras': 'Imóveis',
            
            # ========================================
            # 2️⃣ VEÍCULOS
            # ========================================
            'caminhões': 'Veículos',
            'carros': 'Veículos',
            'embarcações': 'Veículos',
            'motos': 'Veículos',
            'onibus': 'Veículos',
            'peruas': 'Veículos',
            'utilit. pesados': 'Veículos',
            'utilitarios leves': 'Veículos',
            'van leve': 'Veículos',
            'veículos': 'Veículos',
            'bicicleta': 'Veículos',
            
            # ========================================
            # 3️⃣ MÁQUINAS & EQUIPAMENTOS
            # ========================================
            'compressores de ar': 'Máquinas & Equipamentos',
            'empilhadeiras': 'Máquinas & Equipamentos',
            'equip. e mat. industriais': 'Máquinas & Equipamentos',
            'geradores': 'Máquinas & Equipamentos',
            'implementos agrícolas': 'Máquinas & Equipamentos',
            'implementos rod.': 'Máquinas & Equipamentos',
            'terraplenagem': 'Máquinas & Equipamentos',
            'tratores': 'Máquinas & Equipamentos',
            
            # ========================================
            # 4️⃣ TECNOLOGIA
            # ========================================
            'eletricos': 'Tecnologia',
            'informatica': 'Tecnologia',
            'áudio, vídeo e iluminação': 'Tecnologia',
            'eletrodomesticos': 'Tecnologia',  # Alguns eletrodomésticos são tech (TVs, etc)
            
            # ========================================
            # 5️⃣ CASA & CONSUMO
            # ========================================
            'moveis para escritório': 'Casa & Consumo',
            'móveis p/ casa': 'Casa & Consumo',
            'lazer/esportes': 'Casa & Consumo',
            'uso pessoal': 'Casa & Consumo',
            'materiais escolares': 'Casa & Consumo',
            
            # ========================================
            # 6️⃣ INDUSTRIAL & EMPRESARIAL
            # ========================================
            'academia': 'Industrial & Empresarial',
            'esquadrias e estruturas metálicas': 'Industrial & Empresarial',
            'ferramentas': 'Industrial & Empresarial',
            'hospitalar': 'Industrial & Empresarial',
            
            # ========================================
            # 7️⃣ MATERIAIS & SUCATAS
            # ========================================
            'diversos': 'Materiais & Sucatas',
            
            # ========================================
            # 9️⃣ ARTE & COLECIONÁVEIS
            # ========================================
            'instrumentos musicais': 'Arte & Colecionáveis',
            
            # ========================================
            # 🔟 OUTROS
            # ========================================
            'unknown': 'Outros',
        }
    
    def _categorize_item(self, subcategory: str) -> str:
        """
        Mapeia subcategoria original do Sodré para uma das 10 categorias principais
        
        Args:
            subcategory: Subcategoria original (ex: 'carros', 'apartamento', 'informatica')
        
        Returns:
            Uma das 10 categorias principais
        """
        if not subcategory:
            return 'Outros'
        
        # Normaliza
        subcategory_clean = subcategory.lower().strip()
        
        # Busca no mapeamento
        category = self.category_mapping.get(subcategory_clean)
        
        if category:
            return category
        
        # Fallback: tenta detectar pela subcategoria
        if 'imovel' in subcategory_clean or 'imóvel' in subcategory_clean or \
           'apartamento' in subcategory_clean or 'casa' in subcategory_clean or \
           'terreno' in subcategory_clean or 'galpão' in subcategory_clean:
            return 'Imóveis'
        
        if 'carro' in subcategory_clean or 'moto' in subcategory_clean or \
           'caminhão' in subcategory_clean or 'veículo' in subcategory_clean or \
           'veiculo' in subcategory_clean or 'ônibus' in subcategory_clean:
            return 'Veículos'
        
        if 'trator' in subcategory_clean or 'empilhadeira' in subcategory_clean or \
           'gerador' in subcategory_clean or 'compressor' in subcategory_clean or \
           'implemento' in subcategory_clean:
            return 'Máquinas & Equipamentos'
        
        if 'informática' in subcategory_clean or 'informatica' in subcategory_clean or \
           'eletron' in subcategory_clean or 'eletr' in subcategory_clean or \
           'áudio' in subcategory_clean or 'audio' in subcategory_clean:
            return 'Tecnologia'
        
        if 'móvel' in subcategory_clean or 'movel' in subcategory_clean or \
           'lazer' in subcategory_clean or 'esporte' in subcategory_clean:
            return 'Casa & Consumo'
        
        if 'ferramenta' in subcategory_clean or 'industrial' in subcategory_clean or \
           'academia' in subcategory_clean or 'hospitalar' in subcategory_clean:
            return 'Industrial & Empresarial'
        
        if 'sucata' in subcategory_clean or 'material' in subcategory_clean or \
           'diversos' in subcategory_clean:
            return 'Materiais & Sucatas'
        
        if 'instrumento' in subcategory_clean or 'musical' in subcategory_clean or \
           'arte' in subcategory_clean or 'colecionável' in subcategory_clean:
            return 'Arte & Colecionáveis'
        
        # Default
        return 'Outros'
    
    async def scrape(self) -> List[Dict]:
        """Scrape completo com interceptação passiva"""
        print("\n" + "="*60)
        print("🟣 SODRÉ SANTORO - VERSÃO CATEGORIZADA")
        print("="*60)
        
        all_lots = []
        seen_lot_ids = set()  # ✅ Deduplicação na coleta
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                locale='pt-BR'
            )
            
            page = await context.new_page()
            
            current_section = {'name': None, 'api_calls': 0, 'last_capture': 0}
            
            async def intercept_response(response):
                try:
                    if '/api/search-lots' in response.url and response.status == 200:
                        current_section['api_calls'] += 1
                        
                        data = await response.json()
                        per_page = data.get('perPage', 0)
                        
                        if per_page > 0:
                            results = data.get('results', [])
                            hits = data.get('hits', {}).get('hits', [])
                            
                            lots_captured = 0
                            new_lots = 0
                            
                            # Extrai lotes da resposta
                            lots_to_add = []
                            if results:
                                lots_to_add = results
                            elif hits:
                                lots_to_add = [hit.get('_source', hit) for hit in hits]
                            
                            # ✅ Deduplica durante a coleta
                            for lot in lots_to_add:
                                lot_id = lot.get('id') or lot.get('lot_id')
                                if lot_id and lot_id not in seen_lot_ids:
                                    seen_lot_ids.add(lot_id)
                                    all_lots.append(lot)
                                    new_lots += 1
                            
                            if new_lots > 0:
                                current_section['last_capture'] = time.time()
                                section = current_section['name']
                                if section not in self.section_counters:
                                    self.section_counters[section] = 0
                                self.section_counters[section] += new_lots
                                
                                print(f"     📥 API call #{current_section['api_calls']}: +{new_lots} lotes únicos | Total: {self.section_counters[section]}")
                            else:
                                if self.debug:
                                    total = len(lots_to_add)
                                    print(f"     ⚪ API call #{current_section['api_calls']}: 0 novos ({total} duplicatas)")
                except:
                    pass
            
            page.on('response', intercept_response)
            
            for url in self.urls:
                section_name = url.split('/')[3]
                current_section['name'] = section_name
                current_section['api_calls'] = 0
                current_section['last_capture'] = 0
                
                config = self.section_config.get(section_name, {'wait_time': 7, 'max_retries': 3, 'max_pages': 200})
                
                lots_before = len(all_lots)
                
                print(f"\n📦 {section_name.upper()}")
                print(f"  ⏱️ Tempo de espera: {config['wait_time']}s | Máx páginas: {config['max_pages']}")
                
                try:
                    await page.goto(url, wait_until="networkidle", timeout=60000)
                    
                    print(f"  ⏳ Aguardando carregamento inicial...")
                    
                    # ✅ Espera inicial adaptativa
                    for attempt in range(config['max_retries']):
                        await asyncio.sleep(config['wait_time'])
                        
                        lots_after = len(all_lots)
                        new_lots = lots_after - lots_before
                        
                        if new_lots > 0:
                            print(f"  ✅ Tentativa {attempt + 1}: {new_lots} lotes capturados")
                            break
                        else:
                            if attempt < config['max_retries'] - 1:
                                print(f"  🔄 Tentativa {attempt + 1}: Aguardando mais dados...")
                            else:
                                print(f"  ⚠️ Tentativa {attempt + 1}: Nenhum dado capturado")
                    
                    # ✅ PAGINAÇÃO ROBUSTA
                    if len(all_lots) > lots_before:
                        # Contador de tentativas sem sucesso de CLICK (não de dados)
                        failed_clicks = 0
                        max_failed_clicks = 5
                        
                        for page_num in range(2, config['max_pages'] + 1):
                            try:
                                # Scroll
                                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                await asyncio.sleep(1)
                                
                                # Tenta clicar no botão "próxima"
                                button_clicked = False
                                
                                # Seletores possíveis
                                next_selectors = [
                                    'button:has-text("Próxima")',
                                    'button:has-text("próxima")',
                                    'a:has-text("Próxima")',
                                    'a:has-text("próxima")',
                                    '[aria-label*="próxima" i]',
                                    '[aria-label*="next" i]',
                                    'button.pagination-next',
                                    'a.pagination-next',
                                ]
                                
                                for selector in next_selectors:
                                    try:
                                        btn = page.locator(selector).first
                                        if await btn.is_visible(timeout=2000):
                                            await btn.click(timeout=5000)
                                            button_clicked = True
                                            print(f"  ➡️ Página {page_num}...")
                                            break
                                    except:
                                        continue
                                
                                if not button_clicked:
                                    failed_clicks += 1
                                    if failed_clicks >= max_failed_clicks:
                                        print(f"  ✅ {page_num - 1} páginas - fim detectado")
                                        break
                                    continue
                                else:
                                    failed_clicks = 0  # Reset contador
                                
                                # Espera dados novos
                                lots_before_page = len(all_lots)
                                time_waited = 0
                                max_wait = 15
                                
                                while time_waited < max_wait:
                                    await asyncio.sleep(2)
                                    time_waited += 2
                                    
                                    lots_now = len(all_lots)
                                    if lots_now > lots_before_page:
                                        break
                                
                                # Se não capturou nada novo após espera máxima
                                if len(all_lots) == lots_before_page:
                                    print(f"  ⚠️ Página {page_num}: Sem novos dados após {max_wait}s")
                                    failed_clicks += 1
                                    if failed_clicks >= max_failed_clicks:
                                        print(f"  ✅ {page_num - 1} páginas - fim por timeout")
                                        break
                            
                            except Exception as e:
                                if self.debug:
                                    print(f"  ⚠️ Erro na página {page_num}: {e}")
                                failed_clicks += 1
                                if failed_clicks >= max_failed_clicks:
                                    break
                                await asyncio.sleep(2)
                        
                        section_total = len(all_lots) - lots_before
                        print(f"  ✅ TOTAL DA SEÇÃO: {section_total} lotes únicos")
                
                except Exception as e:
                    print(f"  ❌ Erro na seção {section_name}: {e}")
                    self.stats['errors'] += 1
            
            await browser.close()
        
        print(f"\n✅ {len(all_lots)} lotes únicos capturados no total")
        
        # Normaliza dados
        normalized_items = []
        category_stats = {}
        
        for lot in all_lots:
            normalized = self._normalize_lot(lot)
            if normalized:
                normalized_items.append(normalized)
                
                # Atualiza estatísticas por categoria
                cat = normalized.get('categoria', 'Outros')
                if cat not in category_stats:
                    category_stats[cat] = 0
                category_stats[cat] += 1
        
        # Mostra estatísticas por categoria
        print(f"\n📊 Por Categoria Principal:")
        for cat in sorted(category_stats.keys()):
            count = category_stats[cat]
            print(f"  • {cat}: {count} itens")
        
        self.stats['total_scraped'] = len(normalized_items)
        
        return normalized_items
    
    def _normalize_lot(self, lot: Dict) -> Dict:
        """
        Normaliza lote para schema do Supabase + adiciona categoria principal
        """
        try:
            # Extrai subcategoria original
            subcategory = self._safe_str(lot.get('lot_subcategory') or lot.get('subcategory'))
            
            # 🔥 CATEGORIZA com base na subcategoria
            categoria_principal = self._categorize_item(subcategory)
            
            # Conta itens com lances
            if lot.get('lot_has_bid') or lot.get('lot_auction_date_init'):
                self.stats['with_bids'] += 1
            
            item = {
                'source': self.source,
                'external_id': self._safe_str(lot.get('id') or lot.get('lot_id')),
                
                # 🔥 CATEGORIA PRINCIPAL
                'categoria': categoria_principal,
                
                # Básico
                'title': self._safe_str(lot.get('lot_name') or lot.get('name')),
                'description': self._safe_str(lot.get('lot_description') or lot.get('description')),
                'image_url': self._parse_image(lot.get('lot_pictures') or lot.get('image_url')),
                'url': f"{self.base_url}/lote/{lot.get('id')}" if lot.get('id') else None,
                
                # Leilão
                'auction_date': self._parse_datetime(lot.get('lot_auction_date_init')),
                'auction_end_date': self._parse_datetime(lot.get('lot_auction_date_end')),
                'auction_type': self._safe_str(lot.get('auction_type') or lot.get('lot_auction_type')),
                'auctioneer': 'Sodré Santoro',
                
                # Valores
                'current_bid': self._parse_numeric(lot.get('lot_current_value')),
                'minimum_bid': self._parse_numeric(lot.get('lot_minimum_bid')),
                'estimated_value': self._parse_numeric(lot.get('lot_estimated_value')),
                'initial_value': self._parse_numeric(lot.get('lot_initial_value')),
                
                # Status
                'status': self._safe_str(lot.get('lot_status') or lot.get('status')),
                'is_active': lot.get('is_active', True),
                
                # Localização
                'city': self._safe_str(lot.get('lot_city') or lot.get('city')),
                'state': self._safe_str(lot.get('lot_state') or lot.get('state')),
                
                # Veículos
                'vehicle_brand': self._safe_str(lot.get('lot_brand')),
                'vehicle_model': self._safe_str(lot.get('lot_model')),
                'vehicle_year': self._parse_int(lot.get('lot_year')),
                'vehicle_color': self._safe_str(lot.get('lot_color')),
                'vehicle_km': self._parse_int(lot.get('lot_km')),
                'vehicle_plate': self._safe_str(lot.get('lot_plate')),
                'vehicle_fuel': self._safe_str(lot.get('lot_fuel')),
                'lot_optionals': self._parse_optionals(lot.get('lot_optionals')),
                
                # Judicial
                'lot_number': self._safe_str(lot.get('lot_number')),
                'lot_judicial_process': self._safe_str(lot.get('lot_judicial_process')),
                'lot_judicial_vara': self._safe_str(lot.get('lot_judicial_vara')),
                'lot_judicial_district': self._safe_str(lot.get('lot_judicial_district')),
                'lot_judicial_judge': self._safe_str(lot.get('lot_judicial_judge')),
                'tj_praca_value': self._parse_numeric(lot.get('tj_praca_value')),
                'tj_praca_discount': self._parse_numeric(lot.get('tj_praca_discount')),
                
                # Imóveis - endereço
                'lot_neighborhood': self._safe_str(lot.get('lot_neighborhood')),
                'lot_street': self._safe_str(lot.get('lot_street')),
                
                # Imóveis - características
                'lot_dormitories': self._parse_int(lot.get('lot_dormitories')),
                'lot_useful_area': self._parse_numeric(lot.get('lot_useful_area')),
                'lot_total_area': self._parse_numeric(lot.get('lot_total_area')),
                'lot_suites': self._parse_int(lot.get('lot_suites')),
                
                # Materiais - SUBCATEGORIA ORIGINAL
                'lot_subcategory': subcategory,
                'lot_type_name': self._safe_str(lot.get('lot_type_name')),
                
                # Metadata
                'metadata': self._build_metadata(lot),
            }
            
            # Remove None values
            return {k: v for k, v in item.items() if v is not None}
        
        except Exception as e:
            if self.debug:
                print(f"  ⚠️ Erro ao normalizar lote {lot.get('id')}: {e}")
            return None
    
    def _build_metadata(self, lot: Dict) -> Dict:
        """Constrói metadata com campos extras"""
        metadata = {}
        
        # Campos extras
        extra_fields = [
            'segment_base', 'search_terms',
        ]
        
        for field in extra_fields:
            val = lot.get(field)
            if val:
                metadata[field] = val
        
        return metadata if metadata else {}
    
    def _parse_optionals(self, value):
        """Parse lot_optionals para array"""
        if not value:
            return None
        if isinstance(value, list):
            return [str(opt) for opt in value if opt]
        if isinstance(value, str):
            return [value]
        return None
    
    def _parse_image(self, value):
        """Parse image_url ou lot_pictures"""
        if not value:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, list) and len(value) > 0:
            return value[0]
        return None
    
    def _safe_str(self, value) -> str:
        if value is None:
            return None
        try:
            result = str(value).strip()
            return result if result else None
        except:
            return None
    
    def _parse_datetime(self, value) -> str:
        if not value:
            return None
        try:
            if isinstance(value, str):
                value = value.replace('Z', '+00:00')
                if 'T' in value:
                    return value
                try:
                    dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                    return dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')
                except:
                    pass
        except:
            pass
        return None
    
    def _parse_numeric(self, value):
        if value is None:
            return None
        try:
            return float(value)
        except:
            return None
    
    def _parse_int(self, value):
        if value is None:
            return None
        try:
            return int(value)
        except:
            return None


async def main():
    print("\n" + "="*70)
    print("🚀 SODRÉ SANTORO - SCRAPER CATEGORIZADO")
    print("="*70)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = time.time()
    supabase = None
    
    try:
        if SupabaseClient:
            print("\n💓 Iniciando sistema de heartbeat...")
            supabase = SupabaseClient(
                service_name='sodre_scraper',
                service_type='scraper'
            )
            
            if supabase.test():
                supabase.heartbeat_start(metadata={
                    'scraper': 'sodre',
                    'version': 'categorizada',
                })
        
        print("\n🔥 FASE 1: COLETANDO DADOS")
        scraper = SodreScraperCategorizado(debug=False)
        items = await scraper.scrape()
        
        print(f"\n✅ Total coletado: {len(items)} itens")
        print(f"🔥 Itens com lances: {scraper.stats['with_bids']}")
        print(f"⚠️  Erros: {scraper.stats['errors']}")
        
        if not items:
            print("⚠️ Nenhum item coletado")
            if supabase:
                supabase.heartbeat_finish(status='warning', final_stats={
                    'items_collected': 0,
                })
            return
        
        # Salva JSON
        output_dir = Path(__file__).parent / 'data' / 'normalized'
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_file = output_dir / f'sodre_{timestamp}.json'
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        print(f"💾 JSON: {json_file}")
        
        # Insere no Supabase
        if supabase:
            print("\n📤 FASE 2: INSERINDO NO SUPABASE")
            print(f"\n  📤 sodre_items: {len(items)} itens")
            stats = supabase.upsert('sodre_items', items)
            
            print(f"    ✅ Inseridos/Atualizados: {stats['inserted']}")
            if stats.get('duplicates_removed', 0) > 0:
                print(f"    🔄 Duplicatas removidas: {stats['duplicates_removed']}")
            if stats['errors'] > 0:
                print(f"    ⚠️ Erros: {stats['errors']}")
            
            # ✅ Usa heartbeat_success() para manter status='active' e event='completed'
            supabase.heartbeat_success(final_stats={
                'items_collected': len(items),
                'items_inserted': stats['inserted'],
                'items_with_bids': scraper.stats['with_bids'],
                'duplicates_removed': stats.get('duplicates_removed', 0),
            })
    
    except Exception as e:
        print(f"⚠️ Erro crítico: {e}")
        if supabase:
            supabase.heartbeat_error(str(e)[:500])
    
    finally:
        elapsed = time.time() - start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        
        print("\n" + "="*70)
        print("📊 ESTATÍSTICAS FINAIS")
        print("="*70)
        print(f"🟣 Sodré Santoro:")
        print(f"  • Total coletado: {scraper.stats['total_scraped']}")
        print(f"  • Com lances: {scraper.stats['with_bids']}")
        print(f"  • Erros: {scraper.stats['errors']}")
        print(f"\n⏱️ Duração: {minutes}min {seconds}s")
        print(f"✅ Concluído: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    asyncio.run(main())