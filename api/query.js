// Serverless API (Vercel) — corre queries de solo lectura a BigQuery.
// Auth: cuenta de servicio en la env var GCP_SA_KEY (JSON).
// Acceso: opcional, clave compartida en APP_KEY (header x-app-key).
const { BigQuery } = require('@google-cloud/bigquery');

const PROJECT = process.env.GCP_PROJECT || 'gen-lang-client-0913063106';
const CAMP = '`' + PROJECT + '.__DS__.master_campaign_results`';
const ADSET = '`' + PROJECT + '.__DS__.master_ads_ad_results`';
const BUD = '`' + PROJECT + '.__DS__.media_plan_budgets`';
const PLAT = "platform IN ('Facebook','Google','TikTok','Pinterest')";
const PCASE = "CASE platform WHEN 'Facebook' THEN 'meta' WHEN 'Google' THEN 'gads' WHEN 'TikTok' THEN 'ttk' WHEN 'Pinterest' THEN 'pin' END";

// Whitelist de queries. El cliente NUNCA manda SQL: manda un "kind" + params.
const QUERIES = {
  target_get: p => ({ query: "SELECT amount, currency FROM `" + PROJECT + ".cross_clients.media_plan_targets` WHERE client=@client AND month=@month LIMIT 1", params: { client: p.client, month: p.month }, types: { client: 'STRING', month: 'STRING' } }),
  target_set: p => ({ query: "MERGE `" + PROJECT + ".cross_clients.media_plan_targets` T USING (SELECT @client client, @month month, @amount amount, @currency currency) S ON T.client=S.client AND T.month=S.month WHEN MATCHED AND S.amount IS NULL THEN DELETE WHEN MATCHED THEN UPDATE SET amount=S.amount, currency=S.currency, updated_at=CURRENT_TIMESTAMP() WHEN NOT MATCHED AND S.amount IS NOT NULL THEN INSERT (client,month,amount,currency,updated_at) VALUES (S.client,S.month,S.amount,S.currency,CURRENT_TIMESTAMP())", params: { client: p.client, month: p.month, amount: (p.amount==null||p.amount==='')?null:Number(p.amount), currency: p.currency||null }, types: { client: 'STRING', month: 'STRING', amount: 'FLOAT64', currency: 'STRING' } }),
  currency: () => ({ query: "SELECT UPPER(account_currency) AS ccy FROM " + CAMP + " WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND account_currency IS NOT NULL GROUP BY UPPER(account_currency) ORDER BY SUM(cost_raw) DESC LIMIT 1", params: {} }),
  clients: () => ({ query: "WITH ai AS (SELECT client_normalized_name AS cli, ANY_VALUE(vertical) AS vertical, ANY_VALUE(ukelele_group) AS grp, LOGICAL_OR(NOT has_terminated) AS active FROM `" + PROJECT + ".cross_clients.accounts_info` GROUP BY cli), plats AS (SELECT business_name AS cli, STRING_AGG(DISTINCT platform ORDER BY platform) AS platforms FROM `" + PROJECT + ".cross_clients.complete_ads_report` WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) GROUP BY cli) SELECT ai.cli AS client, plats.platforms AS platforms, ai.vertical AS vertical, ai.grp AS grp, ai.active AS active FROM ai LEFT JOIN plats ON ai.cli = plats.cli ORDER BY ai.vertical NULLS LAST, ai.cli", params: {} }),

  pacing_campaigns: p => ({
    query: `SELECT ${PCASE} grp, campaign_name name, SUM(cost_raw) spend
            FROM ${CAMP} WHERE ${PLAT} AND date BETWEEN @from AND @to
            GROUP BY grp, name HAVING spend > 0`,
    params: { from: p.from, to: p.to }
  }),
  pacing_daily: p => ({
    query: `SELECT CAST(date AS STRING) date, ${PCASE} grp, SUM(cost_raw) spend
            FROM ${CAMP} WHERE ${PLAT} AND date BETWEEN @from AND @to
            GROUP BY date, grp`,
    params: { from: p.from, to: p.to }
  }),
  ga4_totals: p => ({
    query: `SELECT SUM(purchase_revenue_raw) rev, SUM(purchase) tx, SUM(session_start) sess
            FROM ${CAMP} WHERE date BETWEEN @from AND @to`,
    params: { from: p.from, to: p.to }
  }),
  results: p => ({
    query: `SELECT ${PCASE} grp, campaign_name name,
              SUM(cost_raw) cons, SUM(impressions) imp, SUM(clicks) clk,
              SUM(purchase_revenue_raw) ga4Rev, SUM(purchase) ga4Tx, SUM(session_start) sess,
              SUM(ads_purchase_revenue_raw) plRev, SUM(ads_purchase) plTx
            FROM ${CAMP} WHERE ${PLAT} AND date BETWEEN @from AND @to
            GROUP BY grp, name HAVING cons > 0`,
    params: { from: p.from, to: p.to }
  }),
  camp_daily: p => ({
    query: `SELECT CAST(date AS STRING) date,
              SUM(cost_raw) cons, SUM(impressions) imp, SUM(clicks) clk,
              SUM(purchase_revenue_raw) ga4Rev, SUM(purchase) ga4Tx, SUM(session_start) sess,
              SUM(ads_purchase_revenue_raw) plRev, SUM(ads_purchase) plTx
            FROM ${CAMP} WHERE campaign_name = @name AND ${PLAT} AND date BETWEEN @from AND @to
            GROUP BY date ORDER BY date`,
    params: { name: p.name, from: p.from, to: p.to }
  }),
  adsets: p => ({
    query: `SELECT ad_set_name name,
              SUM(cost_raw) cons, SUM(impressions) imp, SUM(clicks) clk,
              SUM(ads_purchase_revenue_raw) plRev, SUM(ads_purchase) plTx
            FROM ${ADSET} WHERE campaign_name = @name AND date BETWEEN @from AND @to
            GROUP BY ad_set_name HAVING cons > 0 ORDER BY cons DESC`,
    params: { name: p.name, from: p.from, to: p.to }
  }),
  adset_daily: p => ({
    query: `SELECT CAST(date AS STRING) date,
              SUM(cost_raw) cons, SUM(impressions) imp, SUM(clicks) clk,
              SUM(ads_purchase_revenue_raw) plRev, SUM(ads_purchase) plTx
            FROM ${ADSET} WHERE campaign_name = @camp AND ad_set_name = @adset AND date BETWEEN @from AND @to
            GROUP BY date ORDER BY date`,
    params: { camp: p.camp, adset: p.adset, from: p.from, to: p.to }
  }),
  // --- plan mensual (presupuesto por campaña), compartido por todo el equipo ---
  active_campaigns: p => ({ query: `WITH mx AS (SELECT platform p, MAX(date) d FROM ${CAMP} WHERE ${PLAT} AND date BETWEEN @from AND @to GROUP BY platform) SELECT DISTINCT ${PCASE} grp, campaign_name name FROM ${CAMP} JOIN mx ON platform=mx.p AND date=mx.d WHERE cost_raw>0`, params: { from: p.from, to: p.to } }), active_adsets: p => ({ query: `WITH mx AS (SELECT MAX(date) d FROM ${ADSET} WHERE campaign_name=@name AND date BETWEEN @from AND @to) SELECT DISTINCT ad_set_name name FROM ${ADSET}, mx WHERE campaign_name=@name AND cost_raw>0 AND date=mx.d`, params: { name: p.name, from: p.from, to: p.to } }), budgets_get: p => ({
    query: `SELECT platform, campaign, amount FROM ${BUD} WHERE month = @month`,
    params: { month: p.month }
  }),
  budget_set: p => ({
    query: `MERGE ${BUD} T
            USING (SELECT @month month, @platform platform, @campaign campaign, @amount amount) S
            ON T.month = S.month AND T.platform = S.platform AND T.campaign = S.campaign
            WHEN MATCHED AND S.amount IS NULL THEN DELETE
            WHEN MATCHED THEN UPDATE SET amount = S.amount, updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED AND S.amount IS NOT NULL THEN
              INSERT (month, platform, campaign, amount, updated_at)
              VALUES (S.month, S.platform, S.campaign, S.amount, CURRENT_TIMESTAMP())`,
    params: { month: p.month, platform: p.platform, campaign: p.campaign,
              amount: (p.amount == null || p.amount === '') ? null : Number(p.amount) },
    types: { month: 'STRING', platform: 'STRING', campaign: 'STRING', amount: 'FLOAT64' }
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
    let rows; try { const _r = await client().query(opts); rows = _r[0]; } catch(_e){ if(body.kind==='budgets_get'||String((_e&&_e.message)||'').indexOf('Not found')>=0){ rows=[]; } else { throw _e; } }
    res.status(200).json({ rows });
  } catch (e) {
    res.status(500).json({ error: (e && e.message) || 'Error de query' });
  }
};
