#!/usr/bin/env python3
"""
    MongoDB Models Module
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime

@dataclass
class CategoryModel:
    """Модель категории"""
    
    category_id: str
    partner: str
    name: str
    path: str
    path_array: List[str]
    level: int
    parent_path: Optional[str] = None
    total_products: int = 0
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_mongodb_document(self) -> Dict[str, Any]:
        """Конвертация в документ MongoDB"""
        return {
            "_id": f"{self.partner}_{self.category_id}",
            "partner": self.partner,
            "category_id": self.category_id,
            "name": self.name,
            "path": self.path,
            "path_array": self.path_array,
            "level": self.level,
            "parent_path": self.parent_path,
            "metadata": {
                "total_products": self.total_products,
                "last_updated": datetime.utcnow()
            }
        }
    
    @classmethod
    def from_dataframe_row(cls, row: Dict[str, Any], total_products: int):
        """Создание из строки DataFrame"""
        return cls(
            category_id=str(row['Category_ID']),
            partner=row['Partner_Name'],
            name=row['Category_FullPathName'].split('\\')[-1],
            path=row['Category_FullPathName'].replace('\\', '/'),
            path_array=row['Category_FullPathName'].split('\\'),
            level=row['Category_FullPathName'].count('\\') + 1,
            parent_path=row.get('parent_path'),
            total_products=total_products
        )

@dataclass
class BreadcrumbItem:
    """Элемент хлебных крошек"""
    level: int
    name: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {"level": self.level, "name": self.name}

@dataclass
class CategoryInfo:
    """Информация о категории внутри товара (embedded document)"""
    id: str
    name: str
    full_path: str
    breadcrumbs: List[BreadcrumbItem]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "full_path": self.full_path,
            "breadcrumbs": [item.to_dict() for item in self.breadcrumbs]
        }

@dataclass
class ProductModel:
    """Модель товара"""
    
    offer_id: str
    partner: str
    name: str
    product_type: str
    category_info: CategoryInfo
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_mongodb_document(self) -> Dict[str, Any]:
        """Конвертация в документ MongoDB"""
        return {
            "_id": f"{self.partner}_{self.offer_id}",
            "partner": self.partner,
            "offer_id": self.offer_id,
            "name": self.name,
            "type": self.product_type,
            "category": self.category_info.to_dict(),
            "created_at": self.created_at,
            "updated_at": datetime.utcnow()
        }
    
    @classmethod
    def from_dataframe_row(cls, row: Dict[str, Any]):
        """Создание из строки DataFrame (для товаров)"""
        # Создание breadcrumbs
        path_array = row['Category_FullPathName'].split('\\')
        breadcrumbs = [
            BreadcrumbItem(level=i+1, name=item)
            for i, item in enumerate(path_array)
        ]
        
        category_info = CategoryInfo(
            id=str(row['Category_ID']),
            name=path_array[-1],  # Последний элемент пути
            full_path=row['Category_FullPathName'].replace('\\', '/'),
            breadcrumbs=breadcrumbs
        )
        
        return cls(
            offer_id=str(row['Offer_ID']),
            partner=row['Partner_Name'],
            name=row['Offer_Name'],
            product_type=row['Offer_Type'],
            category_info=category_info
        )

class IndexSpecification:
    """Спецификация индексов MongoDB"""
    
    @staticmethod
    def text_index(field: str) -> Dict[str, Any]:
        return {"keys": [(field, "text")]}
    
    @staticmethod
    def ascending_index(field: str) -> List[tuple]:
        return [(field, 1)]
    
    @staticmethod
    def descending_index(field: str) -> List[tuple]:
        return [(field, -1)]
    
    @staticmethod
    def compound_index(*fields: List[tuple]) -> List[tuple]:
        return list(*fields)

class QueryTemplates:
    """Шаблоны запросов MongoDB"""
    
    @staticmethod
    def by_partner_and_level(partner: str, level: int) -> Dict[str, Any]:
        return {"partner": partner, "level": level}
    
    @staticmethod
    def by_path_array_element(element: str) -> Dict[str, Any]:
        return {"path_array": element}
    
    @staticmethod
    def by_breadcrumb_name(name: str) -> Dict[str, Any]:
        return {"category.breadcrumbs.name": name}
    
    @staticmethod
    def by_product_type_and_category(product_type: str, breadcrumb_name: str) -> Dict[str, Any]:
        return {
            "type": product_type,
            "category.breadcrumbs.name": breadcrumb_name
        }
    
    @staticmethod
    def by_breadcrumb_level_exists(level: int) -> Dict[str, Any]:
        return {f"category.breadcrumbs.{level-1}": {"$exists": True}}

class StatisticsHelper:
    """Вспомогательные методы для статистики MongoDB"""
    
    @staticmethod
    def format_time(milliseconds: float) -> str:
        """Форматирование времени"""
        if milliseconds < 1:
            return f"{milliseconds*1000:.2f} мкс"
        elif milliseconds < 1000:
            return f"{milliseconds:.2f} мсек"
        else:
            return f"{milliseconds/1000:.2f} сек"
    
    @staticmethod
    def format_number(number: int) -> str:
        """Форматирование чисел с разделителями"""
        return f"{number:,}"
    
    @staticmethod
    def calculate_percentage(part: int, total: int) -> float:
        """Расчет процента (для статистики)"""
        if total == 0:
            return 0.0
        return (part / total) * 100
