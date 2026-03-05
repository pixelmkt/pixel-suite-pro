require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

let supabase;
if (SUPABASE_URL && SUPABASE_KEY && SUPABASE_URL !== 'https://your-project.supabase.co') {
    supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
        auth: { persistSession: false }
    });
    console.log('[DB] Supabase connected ✅');
} else {
    console.warn('[DB] Supabase not configured — server starts without DB (add credentials in Railway Variables)');
    const stub = () => ({ data: [], count: 0, error: null });
    const chain = {
        select: () => chain, insert: () => chain, update: () => chain,
        upsert: () => chain, delete: () => chain, eq: () => chain,
        lt: () => chain, lte: () => chain, gt: () => chain, gte: () => chain,
        in: () => chain, order: () => chain, limit: () => chain,
        range: () => stub(), single: () => ({ data: null, error: null }),
        then: (cb) => Promise.resolve(stub()).then(cb),
    };
    supabase = { from: () => chain };
}

module.exports = supabase;
