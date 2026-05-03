import { Schema } from "effect"

export class ApiNotFoundError extends Schema.ErrorClass<ApiNotFoundError>("NotFoundError")(
  {
    name: Schema.Literal("NotFoundError"),
    data: Schema.Struct({
      message: Schema.String,
    }),
  },
  { httpApiStatus: 404 },
) {}

export class ApiDuplicateIDError extends Schema.ErrorClass<ApiDuplicateIDError>("DuplicateIDError")(
  {
    name: Schema.Literal("DuplicateIDError"),
    data: Schema.Struct({
      id: Schema.String,
    }),
  },
  { httpApiStatus: 409 },
) {}

export function notFound(message: string) {
  return new ApiNotFoundError({
    name: "NotFoundError",
    data: { message },
  })
}

export function duplicateID(id: string) {
  return new ApiDuplicateIDError({
    name: "DuplicateIDError",
    data: { id },
  })
}
