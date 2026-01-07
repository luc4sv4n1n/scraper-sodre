#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NORMALIZER FORTALECIDO - Limpeza Avançada de Dados

✨ Recursos:
- Extração de título limpo do external_id (MegaLeilões)
- Captura informações de praça/desconto ANTES de limpar
- Limpeza profunda de texto (HTML, espaços, caracteres especiais)
- Primeira letra maiúscula (Title Case)
- Descrição limpa para análise posterior de IA
- Preserva informações importantes de leilão
"""

import re
from typing import Dict, List, Optional


class UniversalNormalizer:
    """Normalizador com limpeza avançada e captura de metadados"""
    
    VALID_STATES = [
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
        'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN',
        'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
    ]
    
    # Palavras comuns que não devem ter maiúscula inicial
    LOWERCASE_WORDS = {
        'de', 'da', 'do', 'das', 'dos', 'e', 'em', 'com', 'para', 'por', 
        'a', 'o', 'à', 'ao', 'no', 'na', 'um', 'uma'
    }
    
    def normalize(self, item: dict) -> dict:
        """Normaliza item para estrutura uniforme e limpa"""
        
        source = item.get('source', '').lower()
        external_id = item.get('external_id', '')
        raw_title = item.get('title', '')
        raw_description = item.get('description', '')
        
        # ✅ Extrai título limpo do external_id (MegaLeilões)
        if source == 'megaleiloes' and external_id:
            clean_title = self._extract_title_from_external_id(external_id)
        else:
            clean_title = self._clean_title(raw_title, remove_auction_info=True)
        
        # Aplica Title Case inteligente
        clean_title = self._smart_title_case(clean_title)
        
        # Descrição super limpa (remove informações de praça - já vêm do HTML)
        clean_description = self._deep_clean_description(raw_description, remove_auction_info=True)
        
        return {
            # IDs
            'source': item.get('source'),
            'external_id': item.get('external_id'),
            
            # Título limpo e formatado
            'title': clean_title,
            'normalized_title': self._normalize_for_search(clean_title),
            
            # Descrição limpa para análise (MANTÉM informações de praça)
            'description': clean_description,
            'description_preview': self._create_preview(clean_description, clean_title),
            
            # Valores
            'value': self._parse_value(item.get('value')),
            'value_text': item.get('value_text'),
            
            # ✅ INFORMAÇÕES DE PRAÇA (vêm do HTML extraído no scraper)
            'auction_round': item.get('auction_round'),
            'discount_percentage': item.get('discount_percentage'),
            'first_round_value': self._parse_value(item.get('first_round_value')),
            'first_round_date': item.get('first_round_date'),
            
            # Localização
            'city': self._clean_city(item.get('city')),
            'state': self._validate_state(item.get('state')),
            'address': self._clean_address(item.get('address')),
            
            # Leilão
            'auction_date': self._parse_date(item.get('auction_date')),
            'days_remaining': self._parse_days_remaining(item.get('days_remaining')),
            'auction_type': self._clean_text(item.get('auction_type'), 'Leilão'),
            'auction_name': self._clean_text(item.get('auction_name')),
            'store_name': self._clean_text(item.get('store_name')),
            'lot_number': self._clean_text(item.get('lot_number')),
            
            # Estatísticas
            'total_visits': self._parse_int(item.get('total_visits'), 0),
            'total_bids': self._parse_int(item.get('total_bids'), 0),
            'total_bidders': self._parse_int(item.get('total_bidders'), 0),
            
            # Link
            'link': item.get('link'),
            
            # Campos especiais (vehicle_type, property_type, animal_type)
            'vehicle_type': item.get('vehicle_type'),
            'property_type': item.get('property_type'),
            'animal_type': item.get('animal_type'),
            
            # Metadata
            'metadata': self._build_metadata(item),
        }
    
    def _extract_title_from_external_id(self, external_id: str) -> str:
        """
        Extrai título limpo do external_id do MegaLeilões
        
        Input: "megaleiloes_sofa-em-estrutura-macica-tecido-de-veludo-j119233"
        Output: "Sofa Em Estrutura Macica Tecido De Veludo"
        """
        if not external_id:
            return "Sem Título"
        
        # Remove prefixo "megaleiloes_"
        clean = external_id
        if clean.startswith('megaleiloes_'):
            clean = clean[len('megaleiloes_'):]
        
        # Remove código do leilão no final (-jXXXXXX)
        clean = re.sub(r'-j\d+$', '', clean, flags=re.IGNORECASE)
        
        # Remove outros códigos comuns (números longos no final)
        clean = re.sub(r'-\d{5,}$', '', clean)
        
        # Substitui hífens e underscores por espaços
        clean = clean.replace('-', ' ').replace('_', ' ')
        
        # Remove espaços múltiplos
        clean = re.sub(r'\s+', ' ', clean).strip()
        
        # Remove caracteres especiais restantes
        clean = re.sub(r'[^\w\s]', '', clean)
        
        # Limita tamanho
        if len(clean) > 200:
            clean = clean[:197] + '...'
        
        if not clean:
            return "Sem Título"
        
        return clean
    
    def _clean_title(self, title: Optional[str], remove_auction_info: bool = True) -> str:
        """
        Limpeza profunda de título
        remove_auction_info=True: Remove "50% abaixo na 2ª praça" do TÍTULO (já capturado em campo próprio)
        """
        if not title or not str(title).strip():
            return "Sem Título"
        
        clean = str(title).strip()
        
        # Remove "LOTE XX" do início
        clean = re.sub(r'^LOTE\s+\d+\s*[-:–—]?\s*', '', clean, flags=re.IGNORECASE)
        
        # Remove HTML tags
        clean = re.sub(r'<[^>]+>', '', clean)
        
        # Remove entidades HTML
        clean = clean.replace('&nbsp;', ' ')
        clean = clean.replace('&amp;', '&')
        clean = clean.replace('&lt;', '<')
        clean = clean.replace('&gt;', '>')
        clean = clean.replace('&quot;', '"')
        
        # ✅ Remove informações de praça/desconto do TÍTULO (já capturadas em campos próprios)
        if remove_auction_info:
            clean = re.sub(r'\d+%\s*(?:abaixo|desconto|off)?\s*na\s*\d+[ªº]\s*pra[çc]a', '', clean, flags=re.IGNORECASE)
            clean = re.sub(r'\d+[ªº]\s*pra[çc]a', '', clean, flags=re.IGNORECASE)
        
        # Remove vírgulas soltas no final
        clean = clean.rstrip(',').strip()
        
        # Remove "Placa FINAL X (UF)"
        clean = re.sub(r'\s*,?\s*Placa\s+FINAL\s+\d+\s*\([A-Z]{2}\)\s*,?', '', clean, flags=re.IGNORECASE)
        
        # Remove underscores e múltiplos espaços
        clean = clean.replace('_', ' ')
        clean = re.sub(r'\s+', ' ', clean).strip()
        
        # Remove zeros à esquerda de números isolados
        clean = re.sub(r'\b0+(\d{1,2})\b', r'\1', clean)
        
        # Remove valores do título (mantém só no campo value)
        clean = re.sub(r'R\$\s*[\d.,]+', '', clean)
        
        # Remove números de visitas/lances do título
        clean = re.sub(r'\b\d+\s+\d+\s+\d+\b', '', clean)
        
        # Remove espaços múltiplos novamente
        clean = re.sub(r'\s+', ' ', clean).strip()
        
        # Limita tamanho
        if len(clean) > 200:
            clean = clean[:197] + '...'
        
        return clean if clean else "Sem Título"
    
    def _smart_title_case(self, text: str) -> str:
        """
        Aplica Title Case inteligente
        - Primeira letra de cada palavra maiúscula
        - Exceções para preposições (de, da, do, em, com, etc.)
        - Primeira palavra sempre maiúscula
        """
        if not text:
            return text
        
        words = text.split()
        
        if not words:
            return text
        
        # Primeira palavra sempre maiúscula
        result = [words[0].capitalize()]
        
        # Demais palavras
        for word in words[1:]:
            word_lower = word.lower()
            
            # Preserva siglas (ex: USB, HDMI)
            if word.isupper() and len(word) <= 5:
                result.append(word)
            # Preposições e artigos em minúscula
            elif word_lower in self.LOWERCASE_WORDS:
                result.append(word_lower)
            # Demais palavras: primeira maiúscula
            else:
                result.append(word.capitalize())
        
        return ' '.join(result)
    
    def _deep_clean_description(self, description: Optional[str], remove_auction_info: bool = False) -> Optional[str]:
        """
        Limpeza PROFUNDA da descrição
        remove_auction_info=False: MANTÉM informações de praça na descrição (contexto importante)
        
        - Remove HTML tags
        - Remove espaços desnecessários
        - Remove caracteres especiais
        - Remove informações duplicadas
        - Prepara para análise de IA
        """
        if not description:
            return None
        
        desc = str(description).strip()
        
        if not desc or len(desc) < 5:
            return None
        
        # Remove HTML tags (preservando quebras de linha)
        desc = re.sub(r'<br\s*/?>', '\n', desc, flags=re.IGNORECASE)
        desc = re.sub(r'<p>', '\n\n', desc, flags=re.IGNORECASE)
        desc = re.sub(r'</p>', '\n', desc, flags=re.IGNORECASE)
        desc = re.sub(r'<[^>]+>', '', desc)
        
        # Remove entidades HTML
        desc = desc.replace('&nbsp;', ' ')
        desc = desc.replace('&amp;', '&')
        desc = desc.replace('&lt;', '<')
        desc = desc.replace('&gt;', '>')
        desc = desc.replace('&quot;', '"')
        desc = re.sub(r'&#\d+;', '', desc)
        
        # ✅ MANTÉM informações de praça na descrição (remove_auction_info=False por padrão)
        if remove_auction_info:
            desc = re.sub(r'\d+%\s*(?:abaixo|desconto|off)?\s*na\s*\d+[ªº]\s*pra[çc]a', '', desc, flags=re.IGNORECASE)
        
        # Remove múltiplas quebras de linha (máximo 2)
        desc = re.sub(r'\n\s*\n\s*\n+', '\n\n', desc)
        
        # Remove espaços múltiplos
        desc = re.sub(r' {2,}', ' ', desc)
        
        # Remove linhas vazias repetidas
        lines = [line.strip() for line in desc.split('\n')]
        lines = [line for line in lines if line]  # Remove linhas vazias
        desc = '\n'.join(lines)
        
        # Remove informações redundantes comuns
        desc = re.sub(r'Exibindo \d+ de \d+ itens', '', desc, flags=re.IGNORECASE)
        
        # Remove URLs soltas
        desc = re.sub(r'https?://[^\s]+', '', desc)
        
        # Remove emails soltos
        desc = re.sub(r'\S+@\S+', '', desc)
        
        # Remove telefones soltos
        desc = re.sub(r'\(\d{2}\)\s*\d{4,5}-?\d{4}', '', desc)
        
        # Remove espaços extras após limpezas
        desc = re.sub(r'\s+', ' ', desc).strip()
        
        # Limita tamanho (máximo 5000 chars para análise de IA)
        if len(desc) > 5000:
            desc = desc[:4997] + '...'
        
        return desc if desc else None
    
    def _normalize_for_search(self, title: Optional[str]) -> str:
        """Normaliza título para busca (lowercase, sem acentos, sem pontuação)"""
        if not title:
            return ''
        
        normalized = str(title).lower()
        
        # Remove acentos
        replacements = {
            'á': 'a', 'à': 'a', 'â': 'a', 'ã': 'a', 'ä': 'a',
            'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
            'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
            'ó': 'o', 'ò': 'o', 'ô': 'o', 'õ': 'o', 'ö': 'o',
            'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u',
            'ç': 'c', 'ñ': 'n'
        }
        
        for old, new in replacements.items():
            normalized = normalized.replace(old, new)
        
        # Remove tudo que não é letra, número ou espaço
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        
        # Remove espaços múltiplos
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def _create_preview(self, description: Optional[str], title: Optional[str]) -> str:
        """Cria preview curto e limpo"""
        if description and len(description) > 10:
            preview = description[:150].strip()
            if len(description) > 150:
                preview += '...'
            return preview
        
        if title:
            return str(title)[:150]
        
        return "Sem Descrição"
    
    def _parse_value(self, value) -> Optional[float]:
        """Normaliza valor monetário"""
        if value is None:
            return None
        
        try:
            val = float(value)
            if val < 0:
                return None
            return round(val, 2)
        except:
            return None
    
    def _clean_city(self, city: Optional[str]) -> Optional[str]:
        """Formata cidade (Title Case)"""
        if not city:
            return None
        
        city_clean = str(city).strip()
        
        if not city_clean:
            return None
        
        # Remove estado se vier junto
        if '/' in city_clean:
            city_clean = city_clean.split('/')[0].strip()
        
        if '-' in city_clean:
            city_clean = city_clean.split('-')[0].strip()
        
        # Aplica Title Case
        return self._smart_title_case(city_clean)
    
    def _validate_state(self, state: Optional[str]) -> Optional[str]:
        """Valida UF"""
        if not state:
            return None
        
        state_clean = str(state).strip().upper()
        
        if state_clean in self.VALID_STATES:
            return state_clean
        
        return None
    
    def _clean_address(self, address: Optional[str]) -> Optional[str]:
        """Limpa endereço"""
        if not address:
            return None
        
        addr = str(address).strip()
        
        if not addr or len(addr) < 3:
            return None
        
        # Aplica Title Case
        addr = self._smart_title_case(addr)
        
        if len(addr) > 255:
            addr = addr[:252] + '...'
        
        return addr
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """Valida formato ISO de data"""
        if not date_str:
            return None
        
        if isinstance(date_str, str) and 'T' in date_str:
            return date_str
        
        return None
    
    def _parse_days_remaining(self, days) -> Optional[int]:
        """Parse dias restantes"""
        if days is None:
            return None
        
        try:
            days_int = int(days)
            if days_int < 0:
                return 0
            return days_int
        except:
            return None
    
    def _clean_text(self, text: Optional[str], default: Optional[str] = None) -> Optional[str]:
        """Limpa texto genérico"""
        if not text:
            return default
        
        clean = str(text).strip()
        
        if not clean:
            return default
        
        # Aplica Title Case se for texto (não número)
        if not clean.isdigit():
            clean = self._smart_title_case(clean)
        
        if len(clean) > 200:
            clean = clean[:197] + '...'
        
        return clean
    
    def _parse_int(self, value, default: int = 0) -> int:
        """Parse inteiro com default"""
        if value is None:
            return default
        
        try:
            return int(value)
        except:
            return default
    
    def _build_metadata(self, item: dict) -> dict:
        """Constrói metadata preservando campos originais"""
        metadata = item.get('metadata', {}).copy() if isinstance(item.get('metadata'), dict) else {}
        
        # Campos extras vão pro metadata
        extra_fields = [
            'raw_category', 'condition', 'brand', 'model', 'year',
            'quantity', 'unit_price'
        ]
        
        for field in extra_fields:
            if field in item and item[field] is not None:
                metadata[field] = item[field]
        
        return metadata


def normalize_items(items: List[dict]) -> List[dict]:
    """Normaliza lista de itens"""
    normalizer = UniversalNormalizer()
    return [normalizer.normalize(item) for item in items]


def normalize_item(item: dict) -> dict:
    """Normaliza um item único"""
    normalizer = UniversalNormalizer()
    return normalizer.normalize(item)


# ========== TESTE ==========
if __name__ == "__main__":
    print("\n🧪 TESTANDO NORMALIZER - LIMPEZA COMPLETA\n")
    print("="*80)
    
    normalizer = UniversalNormalizer()
    
    test_items = [
        {
            'source': 'megaleiloes',
            'external_id': 'megaleiloes_sofa-em-estrutura-macica-tecido-de-veludo-j119233',
            'title': '50% abaixo na 2ª praça R$ 3.500,00 262 0 Sofá em estrutura maciça...',
            'description': 'Sofá em estrutura maciça revestido em tecido de veludo. Fabricação própria. 50% de desconto na 2ª praça!',
            'auction_round': 2,
            'discount_percentage': 15.0,
            'value': 3500.00,
        },
        {
            'source': 'megaleiloes',
            'external_id': 'megaleiloes_cadeira-odontologica-j119235',
            'title': '40% abaixo na 1ª praça R$ 5.000,00 Cadeira Odontológica',
            'description': 'Cadeira odontológica completa da marca Kavo.',
            'auction_round': 1,
            'value': 5000.00,
        },
    ]
    
    for i, item in enumerate(test_items, 1):
        normalized = normalizer.normalize(item)
        
        print(f"\n{i}. ORIGINAL:")
        print(f"   title (sujo): {item['title'][:80]}...")
        print(f"   description (suja): {item['description'][:80]}...")
        
        print(f"\n   ✨ NORMALIZADO:")
        print(f"   title (limpo): {normalized['title']}")
        print(f"   normalized_title: {normalized['normalized_title']}")
        print(f"   description (limpa): {normalized['description'][:80]}...")
        print(f"   auction_round: {normalized['auction_round']}")
        print(f"   discount_percentage: {normalized['discount_percentage']}")
        print("-" * 80)
    
    print("\n✅ Teste concluído!")