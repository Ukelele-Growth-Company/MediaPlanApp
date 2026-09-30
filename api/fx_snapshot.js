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
    const [cfg] = await bq.query({ query: "SELECT client, reporting_currency FROM \`" + PROJECT + ".cross_clients.media_config\` WHERE use_converted = TRUE", location: 'US' });
    const done = [];
    for (const row of cfg) {
      const ds = row.client; const report = row.reporting_currency;
      if (!/^[a-z0-9_]+$/.test(ds || '')) continue;
      try {
        const [rr] = await bq.query({
          query: "SELECT account_currency native, SAFE_DIVIDE(SUM(cost_raw), SUM(cost)) rate FROM \`" + PROJECT + "." + ds + ".v_media_campaign\` WHERE cost > 0 AND account_currency IS NOT NULL AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) GROUP BY account_currency",
          location: 'US'
        });
        for (const r of rr) {
          if (!r.native || r.native === report || !(r.rate > 0)) continue;
          await bq.query({
            query: "MERGE \`" + PROJECT + ".cross_clients.fx_month_rates\` T USING (SELECT @c client, @n native, @m month, @r rate) S ON T.client=S.client AND T.native=S.native AND T.month=S.month WHEN NOT MATCHED THEN INSERT (client,native,month,rate,source,updated_at) VALUES (S.client,S.native,S.month,S.rate,'auto',CURRENT_TIMESTAMP())",
            params: { c: ds, n: r.native, m: month, r: Number(r.rate) },
            types: { c: 'STRING', n: 'STRING', m: 'STRING', r: 'FLOAT64' },
            location: 'US'
          });
          done.push({ client: ds, native: r.native, rate: Number(r.rate) });
        }
      } catch (e) { done.push({ client: ds, error: (e && e.message) || 'err' }); }
    }
    res.status(200).json({ month: month, ok: true, snapshots: done });
  } catch (e) {
    res.status(500).json({ error: (e && e.message) || 'error' });
  }
};
