import type { NotFoundError as StorageNotFoundError } from "@/storage/storage"
import type { Session } from "@/session/session"
import { Effect } from "effect"
import * as ApiError from "../errors"

type StorageNotFound = InstanceType<typeof StorageNotFoundError>
type DuplicateID = InstanceType<typeof Session.DuplicateIDError>

export function mapStorageNotFound<A, R>(self: Effect.Effect<A, StorageNotFound, R>) {
  return self.pipe(Effect.mapError((error) => ApiError.notFound(error.data.message)))
}

export function mapDuplicateID<A, R>(self: Effect.Effect<A, DuplicateID, R>) {
  return self.pipe(Effect.mapError((error) => ApiError.duplicateID(error.data.id)))
}
