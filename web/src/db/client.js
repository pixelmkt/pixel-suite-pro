require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

// Graceful fallback — server starts even without credentials
// APIs return clear "not configured" errors until you add credentials via Settings panel
let supabase;
if (SUPABASE_URL && SUPABASE_KEY) {
    supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
        auth: { persistSession: false }
    });
} else {
    console.warn('[DB] Supabase not configured — add credentials in Settings panel');
    // Mock client that returns empty data without crashing
    supabase = {
        from: () => ({
            select: () => ({ data: [], count: 0, error: null, order: () => ({ data: [], count: 0, error: null, range: () => ({ data: [], count: 0, error: null }) }) }),
            insert: () => ({ select: () => ({ single: () => ({ data: null, error: { message: 'Supabase not configured. Go to Settings panel.' } }) }) }),
            update: () => ({ eq: () => ({ data: null, error: null }) }),
            upsert: () => ({ select: () => ({ single: () => ({ data: null, error: null }) }) }),
            delete: () => ({ eq: () => ({ data: null, error: null }) }),
            eq: function () { return this },
            lt: function () { return this },
            order: function () { return this },
            range: function () { return { data: [], count: 0 } },
            single: () => ({ data: null, error: null }),
        })
    };
}

module.exports = supabase;
