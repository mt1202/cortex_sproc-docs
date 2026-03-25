CREATE OR REPLACE TABLE PROD_TELEMETRY_DB.BACKUP.PROCEDURE_DOCUMENTATION (
    CATALOG_NAME                    STRING                         NOT NULL,
    SCHEMA_NAME                     STRING                         NOT NULL,
    PROCEDURE_NAME                  STRING                         NOT NULL,
    ARGUMENTS                       STRING                         NOT NULL,
    OBJECT_TYPE                     STRING                         NOT NULL,
    CHANGE_HASH                     STRING                         NOT NULL,

    PROCEDURE_FQN                   STRING                         NOT NULL,
    PROCEDURE_SIGNATURE             STRING                         NOT NULL,
    PROCEDURE_ID                    STRING                         NOT NULL,

    CREATED_ON                      TIMESTAMP_TZ,
    DOCUMENTED_AT                   TIMESTAMP_TZ                   NOT NULL,
    SOURCE_EFFECTIVE_FROM           TIMESTAMP_TZ,
    SOURCE_EFFECTIVE_TO             TIMESTAMP_TZ,
    SOURCE_IS_CURRENT               BOOLEAN                        NOT NULL,

    DOCUMENTATION_STATUS            STRING                         NOT NULL,
    DOCUMENTATION_MODEL             STRING                         NOT NULL,
    DOCUMENTATION_VERSION           STRING                         NOT NULL,

    VERSION_RANK                    NUMBER,
    IS_LATEST_SOURCE_VERSION        BOOLEAN                        NOT NULL,
    IS_LATEST_DOC_VERSION           BOOLEAN                        NOT NULL,
    IS_CURRENT_DOCUMENTATION        BOOLEAN                        NOT NULL,

    LANGUAGE                        STRING,
    RETURNS_TYPE                    STRING,
    EXECUTE_AS                      STRING,
    HANDLER                         STRING,
    RUNTIME_VERSION                 STRING,
    PACKAGES_JSON                   VARIANT,

    SUMMARY                         STRING,
    BUSINESS_PURPOSE                STRING,

    IDEMPOTENCY_CLASSIFICATION      STRING,
    IDEMPOTENCY_EXPLANATION         STRING,
    IDEMPOTENCY_EVIDENCE_JSON       VARIANT,
    IDEMPOTENCY_ASSUMPTIONS_JSON    VARIANT,

    USES_DYNAMIC_SQL                BOOLEAN,
    DYNAMIC_SQL_NOTES               STRING,

    ERROR_HANDLING                  STRING,
    SECURITY_NOTES                  STRING,

    PARAMETERS_JSON                 VARIANT,
    READS_FROM_JSON                 VARIANT,
    WRITES_TO_JSON                  VARIANT,
    CALLS_JSON                      VARIANT,
    CREATES_OBJECTS_JSON            VARIANT,
    LOGIC_STEPS_JSON                VARIANT,
    STEP_BY_STEP_JSON               VARIANT,
    RISKS_JSON                      VARIANT,
    OPEN_QUESTIONS_JSON             VARIANT,

    READ_COUNT                      NUMBER,
    WRITE_COUNT                     NUMBER,
    CALL_COUNT                      NUMBER,
    CREATE_OBJECT_COUNT             NUMBER,
    RISK_COUNT                      NUMBER,
    OPEN_QUESTION_COUNT             NUMBER,
    STEP_COUNT                      NUMBER,

    DOCUMENTATION_JSON              VARIANT                        NOT NULL,
    MARKDOWN_DOC                    STRING                         NOT NULL
);
