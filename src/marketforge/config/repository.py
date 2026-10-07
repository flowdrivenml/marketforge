from __future__ import annotations

from psycopg import Connection

from marketforge.config.models import ProcessingProfile


class ConfigRepository:
    def __init__(
        self,
        conn: Connection,
    ) -> None:
        self.conn = conn

    def list_processing_profiles(
        self,
    ) -> list[ProcessingProfile]:
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    id,
                    name,
                    workers,
                    memory_budget_bytes,
                    scratch_path,
                    scratch_budget_bytes,
                    parquet_row_group_target_bytes,
                    parquet_file_target_bytes,
                    integrity_profile
                FROM config.processing
                ORDER BY name
                """)

            rows = cursor.fetchall()

        return [self._processing_profile_from_row(row) for row in rows]

    def get_processing_profile(
        self,
        name: str,
    ) -> ProcessingProfile | None:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    name,
                    workers,
                    memory_budget_bytes,
                    scratch_path,
                    scratch_budget_bytes,
                    parquet_row_group_target_bytes,
                    parquet_file_target_bytes,
                    integrity_profile
                FROM config.processing
                WHERE name = %s
                """,
                (name,),
            )

            row = cursor.fetchone()

        if row is None:
            return None

        return self._processing_profile_from_row(row)

    def create_processing_profile(
        self,
        profile: ProcessingProfile,
    ) -> ProcessingProfile:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO config.processing (
                    name,
                    workers,
                    memory_budget_bytes,
                    scratch_path,
                    scratch_budget_bytes,
                    parquet_row_group_target_bytes,
                    parquet_file_target_bytes,
                    integrity_profile
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                RETURNING
                    id,
                    name,
                    workers,
                    memory_budget_bytes,
                    scratch_path,
                    scratch_budget_bytes,
                    parquet_row_group_target_bytes,
                    parquet_file_target_bytes,
                    integrity_profile
                """,
                (
                    profile.name,
                    profile.workers,
                    profile.memory_budget_bytes,
                    str(profile.scratch_path),
                    profile.scratch_budget_bytes,
                    profile.parquet_row_group_target_bytes,
                    profile.parquet_file_target_bytes,
                    profile.integrity_profile,
                ),
            )

            row = cursor.fetchone()

        if row is None:
            raise RuntimeError("Processing profile INSERT returned no row")

        return self._processing_profile_from_row(row)

    def update_processing_profile(
        self,
        profile: ProcessingProfile,
    ) -> ProcessingProfile:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE config.processing
                SET
                    workers = %s,
                    memory_budget_bytes = %s,
                    scratch_path = %s,
                    scratch_budget_bytes = %s,
                    parquet_row_group_target_bytes = %s,
                    parquet_file_target_bytes = %s,
                    integrity_profile = %s,
                    updated_at = NOW()
                WHERE name = %s
                RETURNING
                    id,
                    name,
                    workers,
                    memory_budget_bytes,
                    scratch_path,
                    scratch_budget_bytes,
                    parquet_row_group_target_bytes,
                    parquet_file_target_bytes,
                    integrity_profile
                """,
                (
                    profile.workers,
                    profile.memory_budget_bytes,
                    str(profile.scratch_path),
                    profile.scratch_budget_bytes,
                    profile.parquet_row_group_target_bytes,
                    profile.parquet_file_target_bytes,
                    profile.integrity_profile,
                    profile.name,
                ),
            )

            row = cursor.fetchone()

        if row is None:
            raise KeyError(f"Processing profile not found: {profile.name}")

        return self._processing_profile_from_row(row)

    def delete_processing_profile(
        self,
        name: str,
    ) -> bool:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM config.processing
                WHERE name = %s
                """,
                (name,),
            )

            return cursor.rowcount > 0

    @staticmethod
    def _processing_profile_from_row(
        row: dict,
    ) -> ProcessingProfile:
        return ProcessingProfile(
            id=row["id"],
            name=row["name"],
            workers=row["workers"],
            memory_budget_bytes=row["memory_budget_bytes"],
            scratch_path=row["scratch_path"],
            scratch_budget_bytes=row["scratch_budget_bytes"],
            parquet_row_group_target_bytes=(row["parquet_row_group_target_bytes"]),
            parquet_file_target_bytes=(row["parquet_file_target_bytes"]),
            integrity_profile=row["integrity_profile"],
        )
