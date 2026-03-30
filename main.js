import { Actor } from 'apify';
import { CheerioCrawler, RequestQueue } from 'crawlee';

await Actor.init();

// ── Config ────────────────────────────────────────────────────────
const KEYWORD  = 'Senior Data Engineer';
const LOCATION = 'United States';
const MAX_JOBS = 50;

// LinkedIn public search — f_TPR=r86400 = posted in last 24 hours
// Each page returns 25 jobs, so we need pages 0 and 25
const PAGES = [0, 25];

const START_URLS = PAGES.map(start =>
    `https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=${encodeURIComponent(KEYWORD)}&location=${encodeURIComponent(LOCATION)}&f_TPR=r86400&start=${start}`
);

console.log(`🔍 Searching LinkedIn for: "${KEYWORD}" in "${LOCATION}" — last 24 hours`);
console.log(`📄 Fetching pages starting at: ${PAGES.join(', ')}`);

const jobs = [];

const crawler = new CheerioCrawler({
    maxRequestsPerCrawl: 20,
    requestHandlerTimeoutSecs: 90,
    maxConcurrency: 2,

    async requestHandler({ $, request, log }) {
        log.info(`✅ Page loaded: ${request.url}`);

        // LinkedIn job card selectors (public/guest API)
        $('li').each((_, el) => {
            if (jobs.length >= MAX_JOBS) return false;

            const title   = $(el).find('.base-search-card__title, h3').first().text().trim();
            const company = $(el).find('.base-search-card__subtitle, h4').first().text().trim();
            const loc     = $(el).find('.job-search-card__location').first().text().trim();
            const href    = $(el).find('a.base-card__full-link, a').first().attr('href') || '';
            const timeEl  = $(el).find('time');
            const posted  = timeEl.attr('datetime') || timeEl.text().trim() || 'Today';

            if (!title || !company) return;

            jobs.push({
                '#':       jobs.length + 1,
                title,
                company,
                location:  loc  || 'United States',
                posted,
                url:       href ? href.split('?')[0] : '',
                matchScore: scoreJob(title),
                keywords:  getMatchedKeywords(title)
            });
        });

        log.info(`📊 Jobs collected: ${jobs.length}`);
    },

    failedRequestHandler({ request, log }) {
        log.error(`❌ Failed: ${request.url}`);
    }
});

await crawler.run(START_URLS);

// ── Relevance scoring based on Mounish's resume ───────────────────
const RESUME_KEYWORDS = [
    'AWS', 'ETL', 'Python', 'Airflow', 'Redshift', 'Glue', 'PySpark',
    'Spark', 'Snowflake', 'Databricks', 'Terraform', 'SQL', 'DBT',
    'Lambda', 'S3', 'EMR', 'Kafka', 'Kinesis', 'Data Engineer',
    'Data Pipeline', 'Data Warehouse', 'Cloud', 'Azure', 'GCP'
];

function scoreJob(title) {
    const t = title.toLowerCase();
    return RESUME_KEYWORDS.filter(k => t.includes(k.toLowerCase())).length;
}

function getMatchedKeywords(title) {
    const t = title.toLowerCase();
    return RESUME_KEYWORDS.filter(k => t.includes(k.toLowerCase())).join(', ');
}

// Sort by match score (best first)
jobs.sort((a, b) => b.matchScore - a.matchScore);
jobs.forEach((j, i) => { j['#'] = i + 1; });

console.log(`\n✅ Done! Total jobs scraped: ${jobs.length}`);
console.log(`🏆 Top match: ${jobs[0]?.title} @ ${jobs[0]?.company}`);

await Actor.pushData(jobs.slice(0, MAX_JOBS));
await Actor.exit();
