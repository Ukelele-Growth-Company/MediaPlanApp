// Serverless API (Vercel) — corre queries de solo lectura a BigQuery.
// Auth: cuenta de servicio en la env var GCP_SA_KEY (JSON).
// Acceso: opcional, clave compartida en APP_KEY (header x-app-key).
const { BigQuery } = require('@google-cloud/bigquery');

const PROJECT = process.env.GCP_PROJECT || 'gen-lang-client-0913063106';
const CAMP = '`' + PROJECT + '.__DS__.master_campaign_results`';
const ADSET = '`' + PROJECT + '.__DS__.master_ads_ad_results`';
const BUD = '`' + PROJECT + '.__DS__.media_plan_budgets`';
const PLAT = "custom_channel IN ('Facebook Ads','Google Ads','Tik Tok Ads','Pinterest Ads')";
const PCASE = "CASE custom_channel WHEN 'Facebook Ads' THEN 'meta' WHEN 'Google Ads' THEN 'gads' WHEN 'Tik Tok Ads' THEN 'ttk' WHEN 'Pinterest Ads' THEN 'pin' END";

// Whitelist de queries. El cliente NUNCA manda SQL: manda un "kind" + params.
const ADS = '`' + PROJECT + '.cross_clients.complete_ads_report`';
const VCAMP = '`' + PROJECT + '.__DS__.v_media_campaign`';
const VAD = '`' + PROJECT + '.__DS__.v_media_ad`';
const QUERIES = {
  fx_month_get: p => ({
    query: "SELECT native, rate FROM \`" + PROJECT + ".cross_clients.fx_month_rates\` WHERE client=@client AND month=@month",
    params: { client: p.client, month: p.month }, types: { client:'STRING', month:'STRING' }
  }),
  fx_month_set: p => ({
    query: "MERGE \`" + PROJECT + ".cross_clients.fx_month_rates\` T USING (SELECT @client client, @native native, @month month, @rate rate, @source source) S ON T.client=S.client AND T.native=S.native AND T.month=S.month WHEN MATCHED THEN UPDATE SET rate=S.rate, source=S.source, updated_at=CURRENT_TIMESTAMP() WHEN NOT MATCHED THEN INSERT (client,native,month,rate,source,updated_at) VALUES (S.client,S.native,S.month,S.rate,S.source,CURRENT_TIMESTAMP())",
    params: { client:p.client, native:p.native, month:p.month, rate:(p.rate==null?null:Number(p.rate)), source:(p.source||'auto') }, types: { client:'STRING', native:'STRING', month:'STRING', rate:'FLOAT64', source:'STRING' }
  }),
  fx_rates: p => ({
    query: "WITH r AS (SELECT " + PCASE + " grp, MAX(account_currency) native, SUM(cost_raw) raw, SUM(cost) conv FROM " + VCAMP + " WHERE " + PLAT + " AND cost>0 AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) GROUP BY grp) SELECT grp, native, SAFE_DIVIDE(raw,conv) rate, (SELECT MAX(reporting_currency) FROM \`" + PROJECT + ".cross_clients.media_config\` WHERE client='__DS__') report FROM r WHERE native IS NOT NULL",
    params: {}
  }),
  fx_overrides_get: p => ({
    query: "SELECT native, rate FROM \`" + PROJECT + ".cross_clients.fx_overrides\` WHERE client=@client",
    params: { client: p.client }, types: { client:'STRING' }
  }),
  fx_override_set: p => ({
    query: "MERGE \`" + PROJECT + ".cross_clients.fx_overrides\` T USING (SELECT @client client, @native native, @rate rate) S ON T.client=S.client AND T.native=S.native WHEN MATCHED AND S.rate IS NULL THEN DELETE WHEN MATCHED THEN UPDATE SET rate=S.rate, updated_at=CURRENT_TIMESTAMP() WHEN NOT MATCHED AND S.rate IS NOT NULL THEN INSERT (client,native,rate,updated_at) VALUES (S.client,S.native,S.rate,CURRENT_TIMESTAMP())",
    params: { client:p.client, native:p.native, rate:(p.rate==null||p.rate===''?null:Number(p.rate)) }, types: { client:'STRING', native:'STRING', rate:'FLOAT64' }
  }),
  plans_get: p => ({
    query: "SELECT plan_id, name FROM \`" + PROJECT + ".cross_clients.media_plans\` WHERE client=@client ORDER BY created_at",
    params: { client: p.client }, types: { client:'STRING' }
  }),
  plan_add: p => ({
    query: "INSERT INTO \`" + PROJECT + ".cross_clients.media_plans\` (client,plan_id,name,created_at,updated_at) VALUES (@client,@plan,@name,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())",
    params: { client:p.client, plan:p.plan, name:p.name }, types: { client:'STRING', plan:'STRING', name:'STRING' }
  }),
  plan_del: p => ({
    query: "DELETE FROM \`" + PROJECT + ".cross_clients.media_plans\` WHERE client=@client AND plan_id=@plan; DELETE FROM \`" + PROJECT + ".cross_clients.media_plan_assignments\` WHERE client=@client AND plan_id=@plan",
    params: { client:p.client, plan:p.plan }, types: { client:'STRING', plan:'STRING' }
  }),
  assignments_get: p => ({
    query: "SELECT platform, campaign, plan_id FROM \`" + PROJECT + ".cross_clients.media_plan_assignments\` WHERE client=@client",
    params: { client:p.client }, types: { client:'STRING' }
  }),
  assignment_set: p => ({
    query: "MERGE \`" + PROJECT + ".cross_clients.media_plan_assignments\` T USING (SELECT @client client, @platform platform, @campaign campaign, @plan plan_id) S ON T.client=S.client AND T.platform=S.platform AND T.campaign=S.campaign WHEN MATCHED AND (S.plan_id IS NULL OR S.plan_id='general') THEN DELETE WHEN MATCHED THEN UPDATE SET plan_id=S.plan_id, updated_at=CURRENT_TIMESTAMP() WHEN NOT MATCHED AND S.plan_id IS NOT NULL AND S.plan_id!='general' THEN INSERT (client,platform,campaign,plan_id,updated_at) VALUES (S.client,S.platform,S.campaign,S.plan_id,CURRENT_TIMESTAMP())",
    params: { client:p.client, platform:p.platform, campaign:p.campaign, plan:(p.plan||null) }, types: { client:'STRING', platform:'STRING', campaign:'STRING', plan:'STRING' }
  }),
  all_campaigns: p => ({
    query: "SELECT DISTINCT " + PCASE + " grp, campaign_name name FROM " + VCAMP + " WHERE " + PLAT + " AND cost>0 AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)",
    params: {}
  }),
  pacing_adsets: p => ({
    query: "SELECT " + PCASE + " grp, campaign_name name, ad_set_name aset, SUM(cost) cons FROM " + VAD + " WHERE " + PLAT + " AND date BETWEEN @from AND @to GROUP BY grp, name, aset HAVING cons > 0",
    params: { from: p.from, to: p.to }
  }),
  adset_budgets_get: p => ({
    query: "SELECT platform, campaign, ad_set, amount FROM \`" + PROJECT + ".cross_clients.media_plan_adset_budgets\` WHERE client=@client AND month=@month",
    params: { client: p.client, month: p.month },
    types: { client:'STRING', month:'STRING' }
  }),
  adset_budget_set: p => ({
    query: "MERGE \`" + PROJECT + ".cross_clients.media_plan_adset_budgets\` T USING (SELECT @client client, @month month, @platform platform, @campaign campaign, @adset ad_set, @amount amount) S ON T.client=S.client AND T.month=S.month AND T.platform=S.platform AND T.campaign=S.campaign AND T.ad_set=S.ad_set WHEN MATCHED AND S.amount IS NULL THEN DELETE WHEN MATCHED THEN UPDATE SET amount=S.amount, updated_at=CURRENT_TIMESTAMP() WHEN NOT MATCHED AND S.amount IS NOT NULL THEN INSERT (client,month,platform,campaign,ad_set,amount,updated_at) VALUES (S.client,S.month,S.platform,S.campaign,S.ad_set,S.amount,CURRENT_TIMESTAMP())",
    params: { client:p.client, month:p.month, platform:p.platform, campaign:p.campaign, adset:p.adset, amount:(p.amount==null?null:p.amount) },
    types: { client:'STRING', month:'STRING', platform:'STRING', campaign:'STRING', adset:'STRING', amount:'FLOAT64' }
  }),
  planned_get: p => ({ query: "SELECT platform, campaign_name, notas FROM `" + PROJECT + ".cross_clients.planned_campaigns` WHERE client=@client AND month=@month", params: { client: p.client, month: p.month }, types: { client: 'STRING', month: 'STRING' } }),
  planned_add: p => ({ query: "MERGE `" + PROJECT + ".cross_clients.planned_campaigns` T USING (SELECT @client client, @month month, @platform platform, @campaign campaign_name, @notas notas) S ON T.client=S.client AND T.month=S.month AND T.platform=S.platform AND T.campaign_name=S.campaign_name WHEN MATCHED THEN UPDATE SET notas=S.notas, updated_at=CURRENT_TIMESTAMP() WHEN NOT MATCHED THEN INSERT (client,month,platform,campaign_name,notas,created_at,updated_at) VALUES (S.client,S.month,S.platform,S.campaign_name,S.notas,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())", params: { client: p.client, month: p.month, platform: p.platform, campaign: p.campaign, notas: (p.notas||null) }, types: { client: 'STRING', month: 'STRING', platform: 'STRING', campaign: 'STRING', notas: 'STRING' } }),
  planned_del: p => ({ query: "DELETE FROM `" + PROJECT + ".cross_clients.planned_campaigns` WHERE client=@client AND month=@month AND platform=@platform AND campaign_name=@campaign", params: { client: p.client, month: p.month, platform: p.platform, campaign: p.campaign }, types: { client: 'STRING', month: 'STRING', platform: 'STRING', campaign: 'STRING' } }),
  target_get: p => ({ query: "SELECT amount, currency FROM `" + PROJECT + ".cross_clients.media_plan_targets` WHERE client=@client AND month=@month AND plan_id=@plan LIMIT 1", params: { client: p.client, month: p.month, plan: (p.plan||'general') }, types: { client: 'STRING', month: 'STRING', plan: 'STRING' } }),
  target_set: p => ({ query: "MERGE `" + PROJECT + ".cross_clients.media_plan_targets` T USING (SELECT @client client, @month month, @plan plan_id, @amount amount, @currency currency) S ON T.client=S.client AND T.month=S.month AND T.plan_id=S.plan_id WHEN MATCHED AND S.amount IS NULL THEN DELETE WHEN MATCHED THEN UPDATE SET amount=S.amount, currency=S.currency, updated_at=CURRENT_TIMESTAMP() WHEN NOT MATCHED AND S.amount IS NOT NULL THEN INSERT (client,month,plan_id,amount,currency,updated_at) VALUES (S.client,S.month,S.plan_id,S.amount,S.currency,CURRENT_TIMESTAMP())", params: { client: p.client, month: p.month, plan: (p.plan||'general'), amount: (p.amount==null||p.amount==='')?null:Number(p.amount), currency: p.currency||null }, types: { client: 'STRING', month: 'STRING', plan: 'STRING', amount: 'FLOAT64', currency: 'STRING' } }),
  currency: () => ({ query: "SELECT IF(LOGICAL_OR(use_converted), IFNULL((SELECT MAX(reporting_currency) FROM \`" + PROJECT + ".cross_clients.media_config\` WHERE client='__DS__'), 'USD'), UPPER(MAX(account_currency))) AS ccy FROM " + VCAMP + " WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)", params: {} }),
  clients: () => ({ query: "WITH ai AS (SELECT client_normalized_name AS cli, ANY_VALUE(vertical) AS vertical, ANY_VALUE(ukelele_group) AS grp, LOGICAL_OR(NOT has_terminated) AS active FROM `" + PROJECT + ".cross_clients.accounts_info` GROUP BY cli), plats AS (SELECT business_name AS cli, STRING_AGG(DISTINCT platform ORDER BY platform) AS platforms FROM `" + PROJECT + ".cross_clients.complete_ads_report` WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) GROUP BY cli) SELECT ai.cli AS client, plats.platforms AS platforms, ai.vertical AS vertical, ai.grp AS grp, ai.active AS active FROM ai LEFT JOIN plats ON ai.cli = plats.cli ORDER BY ai.vertical NULLS LAST, ai.cli", params: {} }),

  pacing_campaigns: p => ({
    query: `SELECT ${PCASE} grp, campaign_name name, SUM(cost) spend, SUM(cost_raw) rawspend, MAX(account_currency) accy FROM ${VCAMP} WHERE ${PLAT} AND date BETWEEN @from AND @to GROUP BY grp, name HAVING spend > 0`,
    params: { from: p.from, to: p.to }
  }),
  pacing_daily: p => ({
    query: `SELECT CAST(date AS STRING) date, ${PCASE} grp, SUM(cost) spend FROM ${VCAMP} WHERE ${PLAT} AND date BETWEEN @from AND @to GROUP BY date, grp`,
    params: { from: p.from, to: p.to }
  }),
  ga4_totals: p => ({
    query: `SELECT SUM(revenue_ga4) rev, SUM(conversions_ga4) tx, SUM(sessions) sess FROM ${VCAMP} WHERE date BETWEEN @from AND @to`,
    params: { from: p.from, to: p.to }
  }),
  results: p => ({
    query: `SELECT ${PCASE} grp, campaign_name name, SUM(cost) cons, SUM(impressions) imp, SUM(clicks) clk, SUM(revenue_ga4) ga4Rev, SUM(conversions_ga4) ga4Tx, SUM(sessions) sess, SUM(revenue_platform) plRev, SUM(conversions_platform) plTx FROM ${VCAMP} WHERE ${PLAT} AND date BETWEEN @from AND @to GROUP BY grp, name HAVING cons > 0`,
    params: { from: p.from, to: p.to }
  }),
  camp_daily: p => ({
    query: `SELECT CAST(date AS STRING) date, SUM(cost) cons, SUM(impressions) imp, SUM(clicks) clk, SUM(revenue_ga4) ga4Rev, SUM(conversions_ga4) ga4Tx, SUM(sessions) sess, SUM(revenue_platform) plRev, SUM(conversions_platform) plTx FROM ${VCAMP} WHERE campaign_name=@name AND ${PLAT} AND date BETWEEN @from AND @to GROUP BY date ORDER BY date`,
    params: { name: p.name, from: p.from, to: p.to }
  }),
  adsets: p => ({
    query: `SELECT ad_set_name name, SUM(cost) cons, SUM(impressions) imp, SUM(clicks) clk, SUM(revenue) plRev, SUM(conversions) plTx FROM ${VAD} WHERE campaign_name=@name AND date BETWEEN @from AND @to GROUP BY ad_set_name HAVING cons > 0 ORDER BY cons DESC`,
    params: { name: p.name, from: p.from, to: p.to }
  }),
  adset_daily: p => ({
    query: `SELECT CAST(date AS STRING) date, SUM(cost) cons, SUM(impressions) imp, SUM(clicks) clk, SUM(revenue) plRev, SUM(conversions) plTx FROM ${VAD} WHERE campaign_name=@camp AND ad_set_name=@adset AND date BETWEEN @from AND @to GROUP BY date ORDER BY date`,
    params: { camp: p.camp, adset: p.adset, from: p.from, to: p.to }
  }),
  active_campaigns: p => ({
    query: `SELECT DISTINCT ${PCASE} grp, campaign_name name FROM ${VCAMP} WHERE ${PLAT} AND cost>0 AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY)`,
    params: {}
  }),
  active_adsets: p => ({
    query: `WITH mx AS (SELECT MAX(date) d FROM ${VAD} WHERE campaign_name=@name AND date BETWEEN @from AND @to) SELECT DISTINCT ad_set_name name FROM ${VAD}, mx WHERE campaign_name=@name AND cost>0 AND date=mx.d`,
    params: { name: p.name, from: p.from, to: p.to }
  }),
  budgets_get: p => ({
    query: "SELECT platform, campaign, amount FROM `" + PROJECT + ".cross_clients.media_plan_budgets` WHERE client=@client AND plan_id=@plan AND month=@month",
    params: { client: p.client, plan: (p.plan||'general'), month: p.month },
    types: { client:'STRING', plan:'STRING', month:'STRING' }
  }),
  budget_set: p => ({
    query: "MERGE `" + PROJECT + ".cross_clients.media_plan_budgets` T USING (SELECT @client client, @plan plan_id, @month month, @platform platform, @campaign campaign, @amount amount) S ON T.client=S.client AND T.plan_id=S.plan_id AND T.month=S.month AND T.platform=S.platform AND T.campaign=S.campaign WHEN MATCHED AND S.amount IS NULL THEN DELETE WHEN MATCHED THEN UPDATE SET amount=S.amount, updated_at=CURRENT_TIMESTAMP() WHEN NOT MATCHED AND S.amount IS NOT NULL THEN INSERT (client,plan_id,month,platform,campaign,amount,updated_at) VALUES (S.client,S.plan_id,S.month,S.platform,S.campaign,S.amount,CURRENT_TIMESTAMP())",
    params: { client: p.client, plan: (p.plan||'general'), month: p.month, platform: p.platform, campaign: p.campaign, amount: (p.amount == null || p.amount === '') ? null : Number(p.amount) },
    types: { client:'STRING', plan:'STRING', month: 'STRING', platform: 'STRING', campaign: 'STRING', amount: 'FLOAT64' }
  }),
  clone_month: p => ({
    query: "MERGE `" + PROJECT + ".cross_clients.media_plan_budgets` T USING (SELECT client,plan_id,@dst month,platform,campaign,amount FROM `" + PROJECT + ".cross_clients.media_plan_budgets` WHERE client=@client AND month=@src) S ON T.client=S.client AND T.plan_id=S.plan_id AND T.month=S.month AND T.platform=S.platform AND T.campaign=S.campaign WHEN NOT MATCHED THEN INSERT (client,plan_id,month,platform,campaign,amount,updated_at) VALUES (S.client,S.plan_id,S.month,S.platform,S.campaign,S.amount,CURRENT_TIMESTAMP()); MERGE `" + PROJECT + ".cross_clients.media_plan_targets` T USING (SELECT client,@dst month,amount,currency FROM `" + PROJECT + ".cross_clients.media_plan_targets` WHERE client=@client AND month=@src) S ON T.client=S.client AND T.month=S.month WHEN NOT MATCHED THEN INSERT (client,month,amount,currency,updated_at) VALUES (S.client,S.month,S.amount,S.currency,CURRENT_TIMESTAMP()); MERGE `" + PROJECT + ".cross_clients.planned_campaigns` T USING (SELECT client,@dst month,platform,campaign_name,notas FROM `" + PROJECT + ".cross_clients.planned_campaigns` WHERE client=@client AND month=@src) S ON T.client=S.client AND T.month=S.month AND T.platform=S.platform AND T.campaign_name=S.campaign_name WHEN NOT MATCHED THEN INSERT (client,month,platform,campaign_name,notas,created_at,updated_at) VALUES (S.client,S.month,S.platform,S.campaign_name,S.notas,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP()); MERGE `" + PROJECT + ".cross_clients.media_plan_adset_budgets` T USING (SELECT client,@dst month,platform,campaign,ad_set,amount FROM `" + PROJECT + ".cross_clients.media_plan_adset_budgets` WHERE client=@client AND month=@src) S ON T.client=S.client AND T.month=S.month AND T.platform=S.platform AND T.campaign=S.campaign AND T.ad_set=S.ad_set WHEN NOT MATCHED THEN INSERT (client,month,platform,campaign,ad_set,amount,updated_at) VALUES (S.client,S.month,S.platform,S.campaign,S.ad_set,S.amount,CURRENT_TIMESTAMP());",
    params: { client: p.client, src: p.from, dst: p.to },
    types: { client:'STRING', src:'STRING', dst:'STRING' }
  })
};

let _client;
function client() {
  if (_client) return _client;
  if (!process.env.GCP_SA_KEY) throw new Error('Falta la variable de entorno GCP_SA_KEY');
  let creds;
  try { creds = JSON.parse(process.env.GCP_SA_KEY); }
  catch (e) { throw new Error('GCP_SA_KEY no es un JSON valido'); }
  _client = new BigQuery({ projectId: PROJECT, credentials: creds });
  return _client;
}

function readBody(req) {
  return new Promise(resolve => {
    if (req.body && typeof req.body === 'object') return resolve(req.body);
    let d = '';
    req.on('data', c => d += c);
    req.on('end', () => { try { resolve(JSON.parse(d || '{}')); } catch (e) { resolve({}); } });
    req.on('error', () => resolve({}));
  });
}

module.exports = async (req, res) => {
  if (req.method !== 'POST') { res.status(405).json({ error: 'Metodo no permitido' }); return; }
  if (process.env.APP_KEY && req.headers['x-app-key'] !== process.env.APP_KEY) {
    res.status(401).json({ error: 'No autorizado' }); return;
  }
  try {
    const body = await readBody(req);
    const builder = QUERIES[body.kind];
    if (!builder) { res.status(400).json({ error: 'kind invalido' }); return; }
    const { query, params, types } = builder(body.params || {});
    var __ds = (body.params && body.params.client) || 'prune_cl';
    if (!/^[a-z0-9_]+$/.test(__ds)) { res.status(400).json({ error: 'client invalido' }); return; }
    var __q = query.split('__DS__').join(__ds);
    const opts = { query: __q, params, location: 'US' };
    if (types) opts.types = types;
    let rows; try { const _r = await client().query(opts); rows = _r[0]; } catch(_e){ var _isW=/_set$|_add$|_del$|_delete$|_create$|_assign|_upsert|clone/.test(body.kind); if(!_isW&&(body.kind==='budgets_get'||String((_e&&_e.message)||'').indexOf('Not found')>=0)){ rows=[]; } else { throw _e; } }
    res.status(200).json({ rows });
  } catch (e) {
    res.status(500).json({ error: (e && e.message) || 'Error de query' });
  }
};
