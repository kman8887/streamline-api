from marshmallow import Schema, fields


class CreateUserRequestSchema(Schema):
    # Define expected fields here, for now using example placeholders
    email = fields.Email(required=True)
    name = fields.Str(required=True)


class CreateUserResponseSchema(Schema):
    message = fields.Str()
