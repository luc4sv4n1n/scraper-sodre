#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NORMALIZER FORTALECIDO - Limpeza Avançada de Dados
✅ FIX: Aceita formato PostgreSQL timestamptz (Superbid e Sodré)
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
        
        # Extrai título limpo do external_id (MegaLeilões)
        if source == 'megaleiloes' and external_id:
            clean_title = self._extract_title_from_external_id(external_id)
        else:
            clean_title = self._clean_title(raw_title, remove_auction_info=True)
        
        clean_title = self._smart_title_case(clean_title)
        clean_description = self._deep_clean_description(raw_description, remove_auction_info=True)
        
        return {
            # IDs
            'source': item.get('source'),
            'external_id': item.get('external_id'),
            
            # Título limpo
            'title': clean_title,
            'normalized_title': self._normalize_for_search(clean_title),
            
            # Descrição limpa
            'description': clean_description,
            'description_preview': self._create_preview(clean_description, clean_title),
            
            # Valores
            'value': self._parse_value(item.get('value')),
            'value_text': item.get('value_text'),
            
            # Informações de praça
            'auction_round': item.get('auction_round'),
            'discount_percentage': item.get('discount_percentage'),
            'first_round_value': self._parse_value(item.get('first_round_value')),
            'first_round_date': item.get('first_round_date'),
            
            # Localização
            'city': self._clean_city(item.get('city')),
            'state': self._validate_state(item.get('state')),
            'address': self._clean_address(item.get('address')),
            
            # ✅ LEILÃO - COM VALIDAÇÃO CORRIGIDA
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
            
            # Campos especiais (todos os tipos)
            'vehicle_type': item.get('vehicle_type'),
            'property_type': item.get('property_type'),
            'animal_type': item.get('animal_type'),
            'appliance_type': item.get('appliance_type'),
            'tech_type': item.get('tech_type'),
            'parts_type': item.get('parts_type'),
            'specialized_type': item.get('specialized_type'),
            'construction_material_type': item.get('construction_material_type'),
            'consumption_goods_type': item.get('consumption_goods_type'),
            
            # Metadata
            'metadata': self._build_metadata(item),
        }
    
    def _extract_title_from_external_id(self, external_id: str) -> str:
        """Extrai título do external_id do MegaLeilões"""
        if not external_id:
            return "Sem Título"
        
        clean = external_id
        if clean.startswith('megaleiloes_'):
            clean = clean[len('megaleiloes_'):]
        
        clean = re.sub(r'-j\d+$', '', clean, flags=re.IGNORECASE)
        clean = re.sub(r'-\d{5,}$', '', clean)
        clean = clean.replace('-', ' ').replace('_', ' ')
        clean = re.sub(r'\s+', ' ', clean).strip()
        clean = re.sub(r'[^\w\s]', '', clean)
        
        if len(clean) > 200:
            clean = clean[:197] + '...'
        
        return clean if clean else "Sem Título"
    
    def _clean_title(self, title: Optional[str], remove_auction_info: bool = True) -> str:
        """Limpeza profunda de título"""
        if not title or not str(title).strip():
            return "Sem Título"
        
        clean = str(title).strip()
        
        clean = re.sub(r'^LOTE\s+\d+\s*[-:—–]?\s*', '', clean, flags=re.IGNORECASE)
        clean = re.sub(r'<[^>]+>', '', clean)
        clean = clean.replace('&nbsp;', ' ').replace('&amp;', '&')
        
        if remove_auction_info:
            clean = re.sub(r'\d+%\s*(?:abaixo|desconto|off)?\s*na\s*\d+[ªº]\s*pra[çc]a', '', clean, flags=re.IGNORECASE)
            clean = re.sub(r'\d+[ªº]\s*pra[çc]a', '', clean, flags=re.IGNORECASE)
        
        clean = clean.rstrip(',').strip()
        clean = re.sub(r'\s*,?\s*Placa\s+FINAL\s+\d+\s*\([A-Z]{2}\)\s*,?', '', clean, flags=re.IGNORECASE)
        clean = clean.replace('_', ' ')
        clean = re.sub(r'\s+', ' ', clean).strip()
        clean = re.sub(r'R\$\s*[\d.,]+', '', clean)
        
        if len(clean) > 200:
            clean = clean[:197] + '...'
        
        return clean if clean else "Sem Título"
    
    def _smart_title_case(self, text: str) -> str:
        """Aplica Title Case inteligente"""
        if not text:
            return text
        
        words = text.split()
        if not words:
            return text
        
        result = [words[0].capitalize()]
        
        for word in words[1:]:
            word_lower = word.lower()
            
            if word.isupper() and len(word) <= 5:
                result.append(word)
            elif word_lower in self.LOWERCASE_WORDS:
                result.append(word_lower)
            else:
                result.append(word.capitalize())
        
        return ' '.join(result)
    
    def _deep_clean_description(self, description: Optional[str], remove_auction_info: bool = False) -> Optional[str]:
        """Limpeza profunda da descrição"""
        if not description:
            return None
        
        desc = str(description).strip()
        
        if not desc or len(desc) < 5:
            return None
        
        desc = re.sub(r'<br\s*/?>', '\n', desc, flags=re.IGNORECASE)
        desc = re.sub(r'<p>', '\n\n', desc, flags=re.IGNORECASE)
        desc = re.sub(r'</p>', '\n', desc, flags=re.IGNORECASE)
        desc = re.sub(r'<[^>]+>', '', desc)
        
        desc = desc.replace('&nbsp;', ' ').replace('&amp;', '&')
        desc = re.sub(r'&#\d+;', '', desc)
        
        desc = re.sub(r'\n\s*\n\s*\n+', '\n\n', desc)
        desc = re.sub(r' {2,}', ' ', desc)
        
        lines = [line.strip() for line in desc.split('\n') if line.strip()]
        desc = '\n'.join(lines)
        
        desc = re.sub(r'https?://[^\s]+', '', desc)
        desc = re.sub(r'\S+@\S+', '', desc)
        desc = re.sub(r'\(\d{2}\)\s*\d{4,5}-?\d{4}', '', desc)
        
        desc = re.sub(r'\s+', ' ', desc).strip()
        
        if len(desc) > 5000:
            desc = desc[:4997] + '...'
        
        return desc if desc else None
    
    def _normalize_for_search(self, title: Optional[str]) -> str:
        """Normaliza para busca"""
        if not title:
            return ''
        
        normalized = str(title).lower()
        
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
        
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def _create_preview(self, description: Optional[str], title: Optional[str]) -> str:
        """Cria preview"""
        if description and len(description) > 10:
            preview = description[:150].strip()
            if len(description) > 150:
                preview += '...'
            return preview
        
        if title:
            return str(title)[:150]
        
        return "Sem Descrição"
    
    def _parse_value(self, value) -> Optional[float]:
        """Parse valor"""
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
        """Formata cidade"""
        if not city:
            return None
        
        city_clean = str(city).strip()
        
        if not city_clean:
            return None
        
        if '/' in city_clean:
            city_clean = city_clean.split('/')[0].strip()
        
        if '-' in city_clean:
            city_clean = city_clean.split('-')[0].strip()
        
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
        
        addr = self._smart_title_case(addr)
        
        if len(addr) > 255:
            addr = addr[:252] + '...'
        
        return addr
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """
        ✅ FIX: Valida e aceita múltiplos formatos de data/timestamp
        
        Aceita:
        - PostgreSQL: "2026-01-12 09:30:00" (Superbid e Sodré)
        - ISO com T: "2026-01-12T09:30:00"
        - Com timezone: "2026-01-12 09:30:00-03"
        - Apenas data: "2026-01-12"
        """
        if not date_str:
            return None
        
        date_clean = str(date_str).strip()
        
        if not date_clean:
            return None
        
        # Regex: YYYY-MM-DD com ou sem timestamp
        # Aceita espaço OU T entre data e hora
        date_pattern = r'\d{4}-\d{2}-\d{2}(?:[T\s]\d{2}:\d{2}:\d{2})?'
        
        if re.match(date_pattern, date_clean):
            return date_clean
        
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
        """Limpa texto"""
        if not text:
            return default
        
        clean = str(text).strip()
        
        if not clean:
            return default
        
        if not clean.isdigit():
            clean = self._smart_title_case(clean)
        
        if len(clean) > 200:
            clean = clean[:197] + '...'
        
        return clean
    
    def _parse_int(self, value, default: int = 0) -> int:
        """Parse inteiro"""
        if value is None:
            return default
        
        try:
            return int(value)
        except:
            return default
    
    def _build_metadata(self, item: dict) -> dict:
        """Build metadata"""
        metadata = item.get('metadata', {}).copy() if isinstance(item.get('metadata'), dict) else {}
        
        extra_fields = [
            'raw_category', 'condition', 'brand', 'model', 'year',
            'quantity', 'unit_price'
        ]
        
        for field in extra_fields:
            if field in item and item[field] is not None:
                metadata[field] = item[field]
        
        return metadata


def normalize_items(items: List[dict]) -> List[dict]:
    """Normaliza lista"""
    normalizer = UniversalNormalizer()
    return [normalizer.normalize(item) for item in items]


def normalize_item(item: dict) -> dict:
    """Normaliza item único"""
    normalizer = UniversalNormalizer()
    return normalizer.normalize(item)