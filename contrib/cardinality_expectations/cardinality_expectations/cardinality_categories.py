from dataclasses import dataclass

FINITE_CATEGORIES = ["A_FEW", "SEVERAL", "MANY"]
INFINITE_CATEGORIES = ["UNIQUE", "DUPLICATED"]

@dataclass
class CardinalityCategoryProbabilities:
    
    @property
    def predicted_cardinality_category(self):
        raise NotImplementedError

@dataclass
class Depth1CardinalityProbabilities(CardinalityCategoryProbabilities):
    infinite: float
    finite: float

    @property
    def predicted_cardinality_category(self):
        if self.infinite > self.finite:
            return "INFINITE"
        else:
            return "FINITE"

@dataclass
class Depth2CardinalityProbabilities(CardinalityCategoryProbabilities):
    unique: float
    duplicated: float
    a_few: float
    several: float
    many: float

    @property
    def predicted_cardinality_category(self):
        stats = {
            "UNIQUE": self.unique,
            "DUPLICATED": self.duplicated,
            "A_FEW": self.a_few,
            "SEVERAL": self.several,
            "MANY": self.many,
        }
        return max(stats, key=stats.get)


BASE_TYPES = ["NOMINAL_OR_ORDINAL", "QUANTITATIVE", "TEMPORAL"]

#!!! Move this elsewhere
@dataclass
class BaseTypeProbabilities:
    
    nominal_or_ordinal: float
    quantitative: float
    temporal: float

    @property
    def predicted_base_type(self):
        stats = {
            "NOMINAL_OR_ORDINAL": self.nominal_or_ordinal,
            "QUANTITATIVE": self.quantitative,
            "TEMPORAL": self.temporal,
        }
        return max(stats, key=stats.get)
