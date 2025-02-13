
from datetime import date
from marshmallow import Schema, fields

class FinancialElementImportDto:
    label = ""          #Tag.doc
    concept = ""        #Num.tag
    info = ""           #Pre.plabel
    unit = ""           #Num.uom
    value = 0.0         #Num.value

class FinancialsDataDto:
    bs = []  #FinancialElementImportDto[] mapping key from Pre.stmt
    cf = []  #FinancialElementImportDto[] mapping key from Pre.stmt
    ic = []  #FinancialElementImportDto[] mapping key from Pre.stmt

class SymbolFinancialsDto:
    startDate = date.today()   #Sub.period 
    endDate = date.today()     #Sub.period + Sub.fp
    year = 0                   #Sub.fy
    quarter = ""               #Sub.fp
    symbol = ""                #Sub.cik -> Sym.cik -> Sym.symbol
    name = ""                  #Sub.name
    country = ""               #Sub.countryma
    city = ""                  #Sub.cityma
    data = FinancialsDataDto()

class FinancialElementImportSchema(Schema):
    label = fields.String()
    concept = fields.String()
    info = fields.String()
    unit = fields.String()
    value = fields.Int()

class FinancialsDataSchema(Schema):
    bs = fields.List(fields.Nested(FinancialElementImportSchema()))
    cf = fields.List(fields.Nested(FinancialElementImportSchema()))
    ic = fields.List(fields.Nested(FinancialElementImportSchema()))

class SymbolFinancialsSchema(Schema):
    startDate = fields.DateTime()
    endDate = fields.DateTime()
    year = fields.Int()
    quarter = fields.String()
    symbol = fields.String()
    name = fields.String()
    country = fields.String()
    city = fields.String()
    data = fields.Nested(FinancialsDataSchema)