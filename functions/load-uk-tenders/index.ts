const { SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY } = Deno.env.toObject();

const SUPABASE_HEADERS = {
  apikey: SUPABASE_SERVICE_ROLE_KEY,
  Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
  "Content-Type": "application/json",
};

async function getExistingTenderKeys(): Promise<Set<string>> {
  const existingKeys = new Set<string>();
  let page = 0;
  const pageSize = 1000;
  let hasMore = true;

  while (hasMore) {
    const params = new URLSearchParams({
      select: "title,buyer_name",
      offset: String(page * pageSize),
      limit: String(pageSize),
    });

    const res = await fetch(`${SUPABASE_URL}/rest/v1/tenders?${params}`, {
      headers: SUPABASE_HEADERS,
    });

    if (!res.ok) break;
    const batch = await res.json();
    if (batch.length === 0) break;

    for (const row of batch) {
      if (row.title && row.buyer_name) {
        existingKeys.add(`${row.title}|||${row.buyer_name}`);
      }
    }

    hasMore = batch.length === pageSize;
    page++;
  }

  return existingKeys;
}

async function batchInsertTenders(tenders: Record<string, unknown>[]) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/tenders`, {
    method: "POST",
    headers: {
      ...SUPABASE_HEADERS,
      Prefer: "return=minimal",
    },
    body: JSON.stringify(tenders),
  });

  if (!res.ok) {
    const errorText = await res.text();
    console.error("Batch insert error:", errorText);
    return false;
  }

  return true;
}

async function fetchAndInsertTenders() {
  const types = ["Opportunity", "Awarded"];
  const pageSize = 100;
  const now = new Date();
  const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
  let totalInserted = 0;

  const existingTenderKeys = await getExistingTenderKeys();

  for (const noticeType of types) {
    let page = 1;
    let keepGoing = true;
    console.log(`🔍 Fetching '${noticeType}' tenders...`);

    while (keepGoing) {
      console.log(`📦 Fetching page ${page} of '${noticeType}'...`);
      const requestBody = { noticeType, page, pageSize };

      const response = await fetch("https://www.contractsfinder.service.gov.uk/api/rest/2/search_notices/json", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(requestBody),
      });

      const data = await response.json();
      const items = Array.isArray(data.noticeList) ? data.noticeList : [];

      if (items.length === 0) break;

      const batch: Record<string, unknown>[] = [];

      for (const wrapper of items) {
        const item = wrapper.item;
        if (!item || !item.publishedDate) continue;

        const publishedDate = new Date(item.publishedDate);
        if (publishedDate < sevenDaysAgo) {
          keepGoing = false;
          break;
        }

        const title = item.title;
        const buyer_name = item.organisationName;
        const key = `${title}|||${buyer_name}`;

        if (existingTenderKeys.has(key)) {
          console.log(`⚠️ Duplicate found. Skipping: ${title} (${buyer_name})`);
          continue;
        }

        existingTenderKeys.add(key);

        const payload = {
          id: crypto.randomUUID(),
          title,
          buyer_name,
          cpv_category: item.cpvDescription,
          region: item.region,
          value_estimate: item.awardedValue ?? item.valueLow ?? null,
          status: item.noticeStatus,
          closing_date: item.deadlineDate ?? null,
          details_url: item.noticeURL ?? `https://www.contractsfinder.service.gov.uk/notice/${item.id}`,
          awarded_vendor: item.awardedSupplier ?? null,
          notice_type: noticeType,
        };

        console.log(`→ Insert: ${title} | Buyer: ${buyer_name} | Type: ${noticeType}`);
        batch.push(payload);
      }

      if (batch.length > 0) {
        const inserted = await batchInsertTenders(batch);
        if (inserted) totalInserted += batch.length;
      }

      page++;
    }
  }

  console.log(`✅ Inserted ${totalInserted} recent tenders (Opportunity + Awarded).`);
  return totalInserted;
}

// Main execution block
await fetchAndInsertTenders();
