from marshmallow import Schema, fields

class ItemSchema(Schema):
    id = fields.Int(dump_only=True)
    name = fields.Str(required=True)
    description = fields.Str()
    price = fields.Float(required=True)

class ItemUpdateSchema(Schema):
    name = fields.Str(required=True)
    description = fields.Str()
    price = fields.Float()
