"""
# Customer 360 View Builder

## Purpose
Construye una vista unificada de clientes agregando datos de múltiples
sistemas transaccionales y aplicaciones.

## Data Sources
- **CRM System**: Customer profile, contacts, opportunities
- **E-commerce Platform**: Orders, cart abandonment, wishlist
- **Support System**: Tickets, chat history, satisfaction scores
- **Marketing Platform**: Campaigns, email engagement, ad clicks
- **Analytics Platform**: Web behavior, app usage, feature adoption

## Output
Tabla denormalizada `customer_360.customer_profile` con ~200 campos
que alimenta dashboards de negocio y modelos de ML.

## Business Value
- 360° view permite personalización de experiencia
- Identificación de oportunidades de cross-sell/upsell
- Predicción de churn con contexto completo
- Segmentación avanzada para marketing

## Performance
- **Data Volume**: 10M customers × 200 fields = 2B cells
- **Processing Time**: 30-45 minutos
- **Infrastructure**: Spark on EMR (10 × r5.2xlarge)
- **Cost per Run**: ~$15 USD
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='documentation_comprehensive',
    schedule='0 3 * * *',  # 3 AM UTC daily
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'documentation']
) as dag:
    dag.doc_md = __doc__
    
    start = EmptyOperator(task_id='start')
    
    # Extracción paralela de múltiples fuentes
    extract_crm = BashOperator(
        task_id='extract_from_crm',
        bash_command='echo "📥 Extrayendo de CRM"'
    )
    extract_crm.doc_md = """
    ### Extract from CRM (Salesforce)
    
    **API**: Salesforce REST API v57.0  
    **Objects**: Account, Contact, Opportunity, Lead  
    **Method**: Bulk API 2.0 para performance
    
    #### Fields Extracted
    - **Account** (25 campos): Name, Industry, Revenue, etc.
    - **Contact** (30 campos): Name, Email, Phone, Title, etc.
    - **Opportunity** (20 campos): Stage, Amount, Close Date, etc.
    - **Lead** (15 campos): Source, Status, Converted, etc.
    
    #### Incremental Load
    - Filter: `LastModifiedDate >= {{ yesterday_ds }}`
    - Full refresh: Primer día del mes
    - Average records/day: ~50K
    
    **Authentication**: OAuth 2.0 JWT Bearer Flow  
    **Rate Limit**: 100K API calls/24h (monitored)
    """
    
    extract_ecommerce = BashOperator(
        task_id='extract_from_ecommerce',
        bash_command='echo "📥 Extrayendo de E-commerce"'
    )
    extract_ecommerce.doc_md = """
    ### Extract from E-commerce (Shopify)
    
    **API**: Shopify Admin API 2024-01  
    **Endpoints**: /customers, /orders, /abandoned_checkouts
    
    #### Data Points
    - **Customers**: 10M registros
      - Profile, preferences, loyalty tier
    - **Orders**: ~100K nuevos/día
      - Items, totals, shipping, payment
    - **Abandoned Carts**: ~20K/día
      - Items in cart, abandonment reason (inferred)
    
    #### Enrichment
    - Calcular: Lifetime Value (LTV)
    - Calcular: Average Order Value (AOV)
    - Calcular: Purchase Frequency
    - Calcular: Days Since Last Order
    - Calcular: Favorite Categories
    
    **Connection**: MySQL Read Replica (to avoid impacting production)
    """
    
    extract_support = BashOperator(
        task_id='extract_from_support',
        bash_command='echo "📥 Extrayendo de Support"'
    )
    extract_support.doc_md = """
    ### Extract from Support (Zendesk)
    
    **API**: Zendesk Support API v2  
    **Endpoints**: /tickets, /users, /satisfaction_ratings
    
    #### Metrics Calculated
    - Total tickets created
    - Average resolution time
    - Escalation rate
    - Satisfaction score (CSAT)
    - Net Promoter Score (NPS)
    - First Response Time
    - Number of reopened tickets
    
    #### Sentiment Analysis
    - Analyze ticket comments using NLP
    - Classify: Positive / Neutral / Negative
    - Extract topics: Billing, Technical, Shipping, etc.
    - Model: fine-tuned BERT
    
    **Incremental**: Tickets created/updated in last 24h
    """
    
    extract_marketing = BashOperator(
        task_id='extract_from_marketing',
        bash_command='echo "📥 Extrayendo de Marketing"'
    )
    extract_marketing.doc_md = """
    ### Extract from Marketing (HubSpot + Google Ads)
    
    #### HubSpot Data
    - Email campaigns: opens, clicks, conversions
    - Landing pages: visits, form submissions
    - Workflows: enrollment, completion
    - Lists: membership, segmentation
    
    #### Google Ads Data
    - Campaign exposure
    - Click-through rate (CTR)
    - Cost per acquisition (CPA)
    - Conversion attribution
    
    #### Calculated Metrics
    - Marketing Qualified Lead (MQL) score
    - Engagement level (1-10)
    - Channel affinity (email, social, search, etc.)
    - Campaign effectiveness
    
    **APIs**: HubSpot v3, Google Ads API v14
    """
    
    extract_analytics = BashOperator(
        task_id='extract_from_analytics',
        bash_command='echo "📥 Extrayendo de Analytics"'
    )
    extract_analytics.doc_md = """
    ### Extract from Analytics (Google Analytics 4 + Mixpanel)
    
    #### Web Behavior (GA4)
    - Sessions, pageviews, bounce rate
    - Top pages visited
    - Time on site
    - Device types
    - Geographic location
    - Referral sources
    
    #### App Behavior (Mixpanel)
    - App sessions, screen views
    - Feature usage frequency
    - Funnel completion rates
    - Cohort retention
    - Custom event tracking
    
    #### Behavioral Scoring
    - Engagement score (0-100)
    - Product interest signals
    - Intent to purchase score
    - Content preferences
    
    **Data Export**: BigQuery (GA4), Mixpanel Data Pipeline
    """
    
    merge_data = BashOperator(
        task_id='merge_all_sources',
        bash_command='echo "🔗 Consolidando datos"'
    )
    merge_data.doc_md = """
    ### Merge All Data Sources
    
    Combina datos de todas las fuentes usando customer_id como clave.
    
    #### Join Strategy
    ```sql
    SELECT 
        c.customer_id,
        -- CRM fields
        crm.*,
        -- E-commerce aggregates
        ec.total_orders, ec.ltv, ec.aov,
        -- Support metrics
        sp.total_tickets, sp.csat_score, sp.nps,
        -- Marketing engagement
        mk.mql_score, mk.engagement_level,
        -- Analytics behavior
        an.web_sessions, an.app_sessions, an.engagement_score
    FROM customers c
    LEFT JOIN crm_data crm USING (customer_id)
    LEFT JOIN ecommerce_agg ec USING (customer_id)
    LEFT JOIN support_metrics sp USING (customer_id)
    LEFT JOIN marketing_eng mk USING (customer_id)
    LEFT JOIN analytics_behavior an USING (customer_id)
    ```
    
    #### Deduplication
    - Usar email como secondary key
    - Resolver conflictos: preferir registro más reciente
    - Match fuzzy names (Levenshtein distance < 3)
    
    **Output**: 10M rows × 200 columns = ~10GB compressed Parquet
    """
    
    calculate_scores = BashOperator(
        task_id='calculate_derived_scores',
        bash_command='echo "🧮 Calculando scores"'
    )
    calculate_scores.doc_md = """
    ### Calculate Derived Scores & Segments
    
    Calcula scores compuestos y asigna segmentos.
    
    #### Health Score (0-100)
    Combinación ponderada de:
    - Engagement (30%): web + app activity
    - Satisfaction (25%): CSAT + NPS
    - Value (25%): LTV + AOV
    - Recency (20%): days since last interaction
    
    #### Churn Risk Score (0-100)
    Features del modelo:
    - Days since last order
    - Support ticket trend
    - Email engagement decline
    - Web/app activity decline
    - Payment failures
    
    Model: XGBoost trained on 2 years historical data
    
    #### Segmentation
    | Segment | Criteria |
    |---------|----------|
    | Champions | High value + High engagement + Low churn risk |
    | Loyal | High frequency + Good CSAT |
    | At Risk | High value + High churn risk |
    | Hibernating | No activity in 90 days |
    | Lost | No activity in 180 days |
    
    **Output**: customer_id, health_score, churn_risk, segment
    """
    
    load_customer360 = BashOperator(
        task_id='load_to_customer360_table',
        bash_command='echo "📤 Cargando tabla final"'
    )
    load_customer360.doc_md = """
    ### Load to Customer 360 Table
    
    Carga la vista unificada a la tabla de producción.
    
    **Target**: `customer_360.customer_profile`  
    **Database**: Snowflake  
    **Warehouse**: TRANSFORM_WH (X-Large)  
    **Method**: MERGE (upsert)
    
    #### Table Schema
    - **customer_id** (PK): UUID
    - **updated_at**: timestamp
    - **Profile fields**: 50 campos (CRM)
    - **Transaction fields**: 30 campos (E-commerce)
    - **Support fields**: 25 campos (Zendesk)
    - **Marketing fields**: 35 campos (HubSpot + Ads)
    - **Analytics fields**: 40 campos (GA4 + Mixpanel)
    - **Derived scores**: 20 campos (calculados)
    
    #### Clustering
    - Cluster key: (segment, country, updated_date)
    - Mejora query performance en dashboards
    
    #### Downstream Consumers
    - Tableau dashboards (5 dashboards)
    - Looker reports (12 reports)
    - ML pipelines (3 models)
    - Reverse ETL to CRM (sync segments back)
    
    **Post-Load**: Trigger ANALYZE para actualizar statistics
    """
    
    validate_quality = BashOperator(
        task_id='validate_data_quality',
        bash_command='echo "✅ Validando calidad"'
    )
    validate_quality.doc_md = """
    ### Validate Data Quality
    
    Ejecuta chequeos de calidad antes de publicar la tabla.
    
    #### Quality Checks
    1. **Completeness**
       - No hay customer_id NULL
       - Al menos 80% de customers tienen email
       - Al menos 60% tienen datos de todas las fuentes
    
    2. **Freshness**
       - MAX(updated_at) debe ser < 6 horas
       - Al menos 95% de records actualizados en últimas 24h
    
    3. **Accuracy**
       - Total orders count coincide con E-commerce
       - LTV calculation is consistent
       - Segment distribution es razonable (no 100% en un segmento)
    
    4. **Uniqueness**
       - No hay customer_id duplicados
       - Email duplicates < 1%
    
    5. **Referential Integrity**
       - Todos los customer_id existen en dimension table
       - Códigos de país son válidos (ISO 3166)
    
    #### Actions on Failure
    - Severity HIGH: Bloquear publicación, alerta crítica
    - Severity MEDIUM: Publicar con warning, notificar equipo
    - Severity LOW: Publicar, log para revisión
    
    **Tool**: Great Expectations + custom validators
    """
    
    publish_metrics = BashOperator(
        task_id='publish_metrics_to_dashboard',
        bash_command='echo "📊 Publicando métricas"'
    )
    publish_metrics.doc_md = """
    ### Publish Metrics to Dashboard
    
    Publica métricas del pipeline al dashboard de monitoreo.
    
    #### Metrics Published
    - **Volume Metrics**
      - Total customers processed
      - Records per source
      - New customers added
      - Customers updated
    
    - **Performance Metrics**
      - Total execution time
      - Time per stage
      - Data size processed (GB)
      - Spark job metrics (CPU, memory)
    
    - **Quality Metrics**
      - % completeness per field
      - # quality check failures
      - % records with all sources
    
    - **Business Metrics**
      - Segment distribution
      - Average health score
      - Average churn risk
      - High-risk customers count
    
    #### Dashboards
    - **Operational**: Airflow UI, execution metrics
    - **Data Quality**: Monte Carlo dashboard
    - **Business**: Tableau executive dashboard
    
    **Storage**: Metrics stored in `monitoring.pipeline_metrics` table
    """
    
    end = EmptyOperator(task_id='end')
    
    # Pipeline flow
    start >> [extract_crm, extract_ecommerce, extract_support, extract_marketing, extract_analytics]
    [extract_crm, extract_ecommerce, extract_support, extract_marketing, extract_analytics] >> merge_data
    merge_data >> calculate_scores >> load_customer360 >> validate_quality >> publish_metrics >> end
