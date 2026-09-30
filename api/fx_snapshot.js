// Vercel Cron (dia 1 de cada mes): snapshot del tipo de cambio del pipeline por cliente/mes.
const { BigQuery } = require('@google-cloud/bigquery');
const PROJECT = process.env.GCP_PROJECT || 'gen-lang-client-0913063106';

let _client;
function client() {
  if (_client) return _client;
  if (!process.env.GCP_SA_KEY) throw new Error('Falta GCP_SA_KEY');
  _client = new BigQuery({ projectId: PROJECT, credentials: JSON.parse(process.env.GCP_SA_KEY) });
  return _client;
}

module.exports = async (req, res) => {
  if (process.env.CRON_SECRET && req.headers['authorization'] !== 'Bearer ' + process.env.CRON_SECRET) {
    res.status(401).json({ error: 'No autorizado' }); return;
  }
  try {
    const bq = client();
    const month = new Date().toISOString().slice(0, 7);
    const [cfg] = await bq.query({ query: "SELECT client, reporting_currency FROM \`" + PROJECT + ".cross_clients.media_config\` WHERE use_converted = TRUE AND reporting_currency IS NOT NULL", location: 'US' });
    const clients = cfg.filter(c => /^[a-z0-9_]+$/.test(c.client || '') && /^[A-Z]{2,5}$/.test(c.reporting_currency || ''));
    if (!clients.length) { res.status(200).json({ month: month, snapshots: [] }); return; }
    const union = clients.map(c =>
      "SELECT '" + c.client + "' client, '" + c.reporting_currency + "' report, account_currency native, SAFE_DIVIDE(SUM(cost_raw), SUM(cost)) rate FROM \`" + PROJECT + "." + c.client + ".v_media_campaign\` WHERE cost > 0 AND account_currency IS NOT NULL AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) GROUP BY account_currency"
    ).join(" UNION ALL ");
    const [rows] = await bq.query({ query: union, location: 'US' });
    const fx = rows.filter(r => r.native && /^[A-Z]{2,5}$/.test(r.native) && r.native !== r.report && r.rate > 0);
    if (!fx.length) { res.status(200).json({ month: month, snapshots: [] }); return; }
    const using = fx.map(r =>
      "SELECT '" + r.client + "' client, '" + r.native + "' native, '" + month + "' month, " + Number(r.rate) + " rate"
    ).join(" UNION ALL ");
    await bq.query({
      query: "MERGE \`" + PROJECT + ".cross_clients.fx_month_rates\` T USING (" + using + ") S ON T.client=S.client AND T.native=S.native AND T.month=S.month WHEN NOT MATCHED THEN INSERT (client,native,month,rate,source,updated_at) VALUES (S.client,S.native,S.month,S.rate,'auto',CURRENT_TIMESTAMP())",
      location: 'US'
    });
    res.status(200).json({ month: month, snapshots: fx.map(r => ({ client: r.client, native: r.native, rate: Number(r.rate) })) });
  } catch (e) {
    res.status(500).json({ error: (e && e.message) || 'error' });
  }
};
