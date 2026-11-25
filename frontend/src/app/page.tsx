"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import { analyzeEncoding, estimatePrice, PriceEntry } from "@/lib/encoding";

interface TenantOverview {
  id: string;
  name: string;
  slug: string;
  default_priority: string;
  opening_balance: string;
  balance: string;
  ledger_net: string;
  invariant_ok: boolean;
  status_counts: Record<string, number>;
}

interface MessageSummary {
  id: string;
  tenant_id: string;
  tenant_name: string;
  tenant_slug: string;
  client_ref: string | null;
  to: string;
  from: string;
  text: string;
  encoding: string;
  segments: number;
  price_prefix: string;
  cost: string;
  status: string;
  carrier_used: string | null;
  carrier_id: string | null;
  attempt_count: number;
  scheduled_at: string | null;
  accepted_at: string;
  submitted_at: string | null;
  finalized_at: string | null;
}

interface MessageEventItem {
  id: number;
  status: string;
  detail: string | null;
  metadata: Record<string, unknown>;
  occurred_at: string;
}

interface LedgerEntryItem {
  id: number;
  entry_type: string;
  amount: string;
  balance_after: string;
  description: string;
  created_at: string;
}

interface MessageDetail extends MessageSummary {
  units: number;
  unit_price: string;
  priority: string;
  max_attempts: number;
  last_error: string | null;
  queued_at: string | null;
  events: MessageEventItem[];
  ledger_entries: LedgerEntryItem[];
}

const ALL_STATUSES = [
  "ACCEPTED",
  "QUEUED",
  "SUBMITTED",
  "DELIVERED",
  "UNDELIVERED",
  "EXPIRED",
  "FAILED",
];

function statusBadgeClass(status: string): string {
  switch (status) {
    case "DELIVERED":
      return "bg-emerald-500/20 text-emerald-300 border-emerald-500/40";
    case "SUBMITTED":
    case "QUEUED":
      return "bg-sky-500/20 text-sky-300 border-sky-500/40";
    case "ACCEPTED":
      return "bg-indigo-500/20 text-indigo-300 border-indigo-500/40";
    case "UNDELIVERED":
    case "EXPIRED":
      return "bg-amber-500/20 text-amber-300 border-amber-500/40";
    case "FAILED":
      return "bg-rose-500/20 text-rose-300 border-rose-500/40";
    default:
      return "bg-slate-700 text-slate-300 border-slate-600";
  }
}

export default function DashboardPage() {
  const [tenants, setTenants] = useState<TenantOverview[]>([]);
  const [prices, setPrices] = useState<PriceEntry[]>([]);
  const [messages, setMessages] = useState<MessageSummary[]>([]);
  const [totalMessages, setTotalMessages] = useState(0);

  const [loadingOverview, setLoadingOverview] = useState(true);
  const [loadingMessages, setLoadingMessages] = useState(true);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [sseStatus, setSseStatus] = useState<"connected" | "reconnecting">("reconnecting");

  // Filters
  const [filterTenantId, setFilterTenantId] = useState("");
  const [filterStatus, setFilterStatus] = useState("");
  const [filterClientRef, setFilterClientRef] = useState("");
  const [filterStartDate, setFilterStartDate] = useState("");
  const [filterEndDate, setFilterEndDate] = useState("");

  // Send Test Form
  const [sendTenantSlug, setSendTenantSlug] = useState("acme-bank");
  const [sendTo, setSendTo] = useState("+8801711000001");
  const [sendFrom, setSendFrom] = useState("ACME");
  const [sendClientRef, setSendClientRef] = useState("");
  const [sendIdemKey, setSendIdemKey] = useState("");
  const [sendScheduledAt, setSendScheduledAt] = useState("");
  const [sendText, setSendText] = useState("Your DCC verification OTP is 482910.");
  const [sending, setSending] = useState(false);
  const [sendFeedback, setSendFeedback] = useState<{ ok: boolean; text: string } | null>(null);

  // Detail Modal
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [selectedDetail, setSelectedDetail] = useState<MessageDetail | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);

  const fetchOverview = useCallback(async () => {
    try {
      const res = await fetch("/api/gateway/internal/dashboard/overview", { cache: "no-store" });
      if (!res.ok) throw new Error(`Overview HTTP ${res.status}`);
      const data = await res.json();
      setTenants(data.tenants || []);
      setPrices(data.prices || []);
      setErrorMsg(null);
    } catch (err) {
      setErrorMsg(err instanceof Error ? err.message : "Failed to load overview");
    } finally {
      setLoadingOverview(false);
    }
  }, []);

  const fetchMessages = useCallback(async () => {
    setLoadingMessages(true);
    try {
      const params = new URLSearchParams({ limit: "50", offset: "0" });
      if (filterTenantId) params.set("tenant_id", filterTenantId);
      if (filterStatus) params.set("status", filterStatus);
      if (filterClientRef.trim()) params.set("client_ref", filterClientRef.trim());
      if (filterStartDate) params.set("start_date", new Date(filterStartDate).toISOString());
      if (filterEndDate) params.set("end_date", new Date(filterEndDate).toISOString());

      const res = await fetch(`/api/gateway/internal/dashboard/messages?${params.toString()}`, {
        cache: "no-store",
      });
      if (!res.ok) throw new Error(`Messages HTTP ${res.status}`);
      const data = await res.json();
      setMessages(data.messages || []);
      setTotalMessages(data.total || 0);
      setErrorMsg(null);
    } catch (err) {
      setErrorMsg(err instanceof Error ? err.message : "Failed to load messages");
    } finally {
      setLoadingMessages(false);
    }
  }, [filterTenantId, filterStatus, filterClientRef, filterStartDate, filterEndDate]);

  const fetchDetail = useCallback(async (id: string) => {
    setLoadingDetail(true);
    try {
      const res = await fetch(`/api/gateway/internal/dashboard/messages/${id}`, { cache: "no-store" });
      if (!res.ok) throw new Error(`Detail HTTP ${res.status}`);
      setSelectedDetail(await res.json());
    } catch {
      setSelectedDetail(null);
    } finally {
      setLoadingDetail(false);
    }
  }, []);

  useEffect(() => {
    fetchOverview();
  }, [fetchOverview]);

  useEffect(() => {
    fetchMessages();
  }, [fetchMessages]);

  useEffect(() => {
    if (selectedId) {
      fetchDetail(selectedId);
    } else {
      setSelectedDetail(null);
    }
  }, [selectedId, fetchDetail]);

  // Live SSE updates (debounced refresh on message state changes)
  useEffect(() => {
    const es = new EventSource("/api/gateway/internal/dashboard/events");
    let timer: ReturnType<typeof setTimeout> | null = null;

    es.onopen = () => setSseStatus("connected");
    es.onerror = () => setSseStatus("reconnecting");
    es.onmessage = (evt) => {
      try {
        const parsed = JSON.parse(evt.data);
        if (parsed.type === "connected") {
          setSseStatus("connected");
          return;
        }
        if (timer) clearTimeout(timer);
        timer = setTimeout(() => {
          fetchOverview();
          fetchMessages();
          if (selectedId && parsed.message_id === selectedId) {
            fetchDetail(selectedId);
          }
        }, 180);
      } catch {
        // ignore keepalive parse
      }
    };

    return () => {
      if (timer) clearTimeout(timer);
      es.close();
    };
  }, [fetchOverview, fetchMessages, fetchDetail, selectedId]);

  const encodingEstimate = useMemo(() => analyzeEncoding(sendText), [sendText]);
  const priceEstimate = useMemo(
    () => estimatePrice(sendTo.trim(), encodingEstimate.segments, prices),
    [sendTo, encodingEstimate.segments, prices]
  );

  const handleSendTest = async (e: React.FormEvent) => {
    e.preventDefault();
    setSending(true);
    setSendFeedback(null);
    try {
      const body: Record<string, unknown> = {
        tenant_slug: sendTenantSlug,
        to: sendTo.trim(),
        from: sendFrom.trim(),
        text: sendText,
      };
      if (sendClientRef.trim()) body.client_ref = sendClientRef.trim();
      if (sendIdemKey.trim()) body.idempotency_key = sendIdemKey.trim();
      if (sendScheduledAt) body.scheduled_at = new Date(sendScheduledAt).toISOString();

      const res = await fetch("/api/gateway/internal/dashboard/send", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await res.json();
      if (!res.ok) {
        const detail = typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail);
        setSendFeedback({ ok: false, text: `Error ${res.status}: ${detail}` });
      } else {
        setSendFeedback({
          ok: true,
          text: `Accepted ${data.message_id} (${data.encoding}, ${data.segments} seg, $${data.cost})`,
        });
        fetchOverview();
        fetchMessages();
      }
    } catch (err) {
      setSendFeedback({ ok: false, text: err instanceof Error ? err.message : "Network error" });
    } finally {
      setSending(false);
    }
  };

  return (
    <main className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
      {/* Header */}
      <header className="mb-6 flex flex-wrap items-center justify-between gap-4 border-b border-slate-800 pb-4">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-white">
            DCC Mini SMS Gateway — Operations Console
          </h1>
          <p className="text-sm text-slate-400">
            Multi-tenant prepaid ledger, priority fair dispatch &amp; real-time DLR monitoring
          </p>
        </div>
        <div className="flex items-center gap-3">
          <span
            className={`inline-flex items-center gap-2 rounded-full border px-3 py-1 text-xs font-medium ${
              sseStatus === "connected"
                ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-300"
                : "border-amber-500/40 bg-amber-500/10 text-amber-300"
            }`}
          >
            <span
              className={`h-2 w-2 rounded-full ${
                sseStatus === "connected" ? "bg-emerald-400 animate-pulse" : "bg-amber-400"
              }`}
            />
            {sseStatus === "connected" ? "SSE Live Connected" : "SSE Reconnecting"}
          </span>
          <button
            onClick={() => {
              fetchOverview();
              fetchMessages();
            }}
            className="rounded-lg border border-slate-700 bg-slate-800 px-3 py-1.5 text-xs font-medium text-slate-200 hover:bg-slate-700"
          >
            Refresh
          </button>
        </div>
      </header>

      {/* Global Error Banner */}
      {errorMsg && (
        <div className="mb-6 flex items-center justify-between rounded-lg border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-200">
          <span>{errorMsg}</span>
          <button
            onClick={() => {
              fetchOverview();
              fetchMessages();
            }}
            className="rounded bg-rose-500/20 px-2.5 py-1 text-xs font-semibold hover:bg-rose-500/30"
          >
            Retry
          </button>
        </div>
      )}

      {/* Tenant Overview Cards */}
      <section className="mb-6 grid grid-cols-1 gap-4 md:grid-cols-2">
        {loadingOverview ? (
          <>
            <div className="h-44 animate-pulse rounded-xl border border-slate-800 bg-slate-900/70" />
            <div className="h-44 animate-pulse rounded-xl border border-slate-800 bg-slate-900/70" />
          </>
        ) : tenants.length === 0 ? (
          <div className="col-span-2 rounded-xl border border-slate-800 bg-slate-900 p-6 text-center text-sm text-slate-400">
            No tenants seeded.
          </div>
        ) : (
          tenants.map((t) => (
            <div
              key={t.id}
              className="rounded-xl border border-slate-800 bg-slate-900/80 p-5 shadow-sm"
            >
              <div className="flex items-start justify-between">
                <div>
                  <div className="flex items-center gap-2">
                    <h2 className="text-lg font-semibold text-white">{t.name}</h2>
                    <span className="rounded bg-slate-800 px-2 py-0.5 text-xs uppercase text-slate-300">
                      {t.default_priority} priority
                    </span>
                  </div>
                  <p className="mt-0.5 text-xs text-slate-400">
                    Opening: ${t.opening_balance} | Ledger Net: ${t.ledger_net}
                  </p>
                </div>
                <div className="text-right">
                  <div className="text-2xl font-bold text-emerald-400">${t.balance}</div>
                  <span
                    className={`text-[11px] font-medium ${
                      t.invariant_ok ? "text-emerald-400" : "text-rose-400"
                    }`}
                  >
                    {t.invariant_ok ? "Ledger Invariant OK" : "Ledger Mismatch!"}
                  </span>
                </div>
              </div>
              <div className="mt-4 flex flex-wrap gap-2">
                {ALL_STATUSES.map((st) => (
                  <div
                    key={st}
                    className={`rounded-md border px-2 py-1 text-xs font-medium ${statusBadgeClass(st)}`}
                  >
                    {st}: <span className="font-bold">{t.status_counts[st] || 0}</span>
                  </div>
                ))}
              </div>
            </div>
          ))
        )}
      </section>

      {/* Send Test Message Panel */}
      <section className="mb-6 rounded-xl border border-slate-800 bg-slate-900/80 p-5">
        <h2 className="mb-3 text-base font-semibold text-white">Send Test Message</h2>
        <form onSubmit={handleSendTest} className="space-y-4">
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-5">
            <div>
              <label className="mb-1 block text-xs text-slate-400">Tenant</label>
              <select
                value={sendTenantSlug}
                onChange={(e) => {
                  setSendTenantSlug(e.target.value);
                  setSendFrom(e.target.value === "acme-bank" ? "ACME" : "SHOPFAST");
                }}
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-white"
              >
                <option value="acme-bank">Acme Bank (OTP / High)</option>
                <option value="shopfast">ShopFast (Bulk / Normal)</option>
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs text-slate-400">To (E.164)</label>
              <input
                type="text"
                value={sendTo}
                onChange={(e) => setSendTo(e.target.value)}
                placeholder="+8801711000001"
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-white"
                required
              />
            </div>
            <div>
              <label className="mb-1 block text-xs text-slate-400">From (Sender ID)</label>
              <input
                type="text"
                value={sendFrom}
                onChange={(e) => setSendFrom(e.target.value)}
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-white"
                required
              />
            </div>
            <div>
              <label className="mb-1 block text-xs text-slate-400">Client Ref (optional)</label>
              <input
                type="text"
                value={sendClientRef}
                onChange={(e) => setSendClientRef(e.target.value)}
                placeholder="ref-123"
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-white"
              />
            </div>
            <div>
              <label className="mb-1 block text-xs text-slate-400">Scheduled At (optional)</label>
              <input
                type="datetime-local"
                value={sendScheduledAt}
                onChange={(e) => setSendScheduledAt(e.target.value)}
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-white"
              />
            </div>
          </div>

          <div>
            <div className="mb-1 flex items-center justify-between">
              <label className="text-xs text-slate-400">
                Message Text (try GSM-7 extension chars like €{"{}"} or Bangla/emoji for UCS-2)
              </label>
              <input
                type="text"
                value={sendIdemKey}
                onChange={(e) => setSendIdemKey(e.target.value)}
                placeholder="Optional Idempotency-Key"
                className="w-56 rounded border border-slate-700 bg-slate-950 px-2 py-1 text-xs text-white"
              />
            </div>
            <textarea
              rows={2}
              value={sendText}
              onChange={(e) => setSendText(e.target.value)}
              className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-white"
              required
            />
          </div>

          {/* Live GSM-7 / UCS-2 & Price Bar */}
          <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-slate-800 bg-slate-950 px-4 py-2.5 text-xs">
            <div className="flex flex-wrap items-center gap-4">
              <span>
                Encoding:{" "}
                <strong
                  className={
                    encodingEstimate.encoding === "GSM7" ? "text-emerald-300" : "text-amber-300"
                  }
                >
                  {encodingEstimate.encoding}
                </strong>
              </span>
              <span>
                Units: <strong className="text-white">{encodingEstimate.units}</strong> (limit{" "}
                {encodingEstimate.segments <= 1
                  ? encodingEstimate.singleLimit
                  : `${encodingEstimate.multiLimit}/seg`}
                )
              </span>
              <span>
                Segments: <strong className="text-white">{encodingEstimate.segments}</strong>
              </span>
              {priceEstimate ? (
                <span>
                  Price:{" "}
                  <strong className="text-emerald-300">
                    ${priceEstimate.total.toFixed(4)}
                  </strong>{" "}
                  <span className="text-slate-400">
                    ({priceEstimate.prefix} {priceEstimate.country} @ $
                    {priceEstimate.unitPrice.toFixed(4)}/seg)
                  </span>
                </span>
              ) : (
                <span className="text-amber-400">
                  Enter valid priced E.164 (+880, +971, +1, +44) to estimate cost
                </span>
              )}
            </div>
            <button
              type="submit"
              disabled={sending}
              className="rounded-lg bg-indigo-600 px-4 py-2 text-xs font-semibold text-white hover:bg-indigo-500 disabled:opacity-50"
            >
              {sending ? "Sending..." : "Send Test SMS"}
            </button>
          </div>

          {sendFeedback && (
            <div
              className={`rounded-lg border px-3 py-2 text-xs ${
                sendFeedback.ok
                  ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-200"
                  : "border-rose-500/40 bg-rose-500/10 text-rose-200"
              }`}
            >
              {sendFeedback.text}
            </div>
          )}
        </form>
      </section>

      {/* Filterable Messages Table */}
      <section className="rounded-xl border border-slate-800 bg-slate-900/80 p-5">
        <div className="mb-4 flex flex-wrap items-end justify-between gap-3">
          <div>
            <h2 className="text-base font-semibold text-white">
              Messages ({totalMessages})
            </h2>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <select
              value={filterTenantId}
              onChange={(e) => setFilterTenantId(e.target.value)}
              className="rounded-lg border border-slate-700 bg-slate-950 px-2.5 py-1.5 text-xs text-white"
            >
              <option value="">All Tenants</option>
              {tenants.map((t) => (
                <option key={t.id} value={t.id}>
                  {t.name}
                </option>
              ))}
            </select>
            <select
              value={filterStatus}
              onChange={(e) => setFilterStatus(e.target.value)}
              className="rounded-lg border border-slate-700 bg-slate-950 px-2.5 py-1.5 text-xs text-white"
            >
              <option value="">All Statuses</option>
              {ALL_STATUSES.map((st) => (
                <option key={st} value={st}>
                  {st}
                </option>
              ))}
            </select>
            <input
              type="text"
              value={filterClientRef}
              onChange={(e) => setFilterClientRef(e.target.value)}
              placeholder="Filter client_ref..."
              className="w-36 rounded-lg border border-slate-700 bg-slate-950 px-2.5 py-1.5 text-xs text-white"
            />
            <input
              type="date"
              value={filterStartDate}
              onChange={(e) => setFilterStartDate(e.target.value)}
              className="rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-xs text-white"
            />
            <input
              type="date"
              value={filterEndDate}
              onChange={(e) => setFilterEndDate(e.target.value)}
              className="rounded-lg border border-slate-700 bg-slate-950 px-2 py-1.5 text-xs text-white"
            />
          </div>
        </div>

        {loadingMessages ? (
          <div className="space-y-2 py-4">
            <div className="h-10 animate-pulse rounded bg-slate-800/70" />
            <div className="h-10 animate-pulse rounded bg-slate-800/70" />
            <div className="h-10 animate-pulse rounded bg-slate-800/70" />
          </div>
        ) : messages.length === 0 ? (
          <div className="rounded-lg border border-dashed border-slate-700 py-10 text-center text-sm text-slate-400">
            No messages match the selected filters. Send a test message above to get started.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead className="border-b border-slate-800 text-slate-400">
                <tr>
                  <th className="py-2 pr-3">Status</th>
                  <th className="py-2 pr-3">Tenant</th>
                  <th className="py-2 pr-3">To / From</th>
                  <th className="py-2 pr-3">Encoding</th>
                  <th className="py-2 pr-3">Cost</th>
                  <th className="py-2 pr-3">Carrier</th>
                  <th className="py-2 pr-3">Accepted At</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/80">
                {messages.map((m) => (
                  <tr
                    key={m.id}
                    onClick={() => setSelectedId(m.id)}
                    className="cursor-pointer hover:bg-slate-800/50"
                  >
                    <td className="py-2.5 pr-3">
                      <span
                        className={`inline-block rounded border px-2 py-0.5 font-medium ${statusBadgeClass(
                          m.status
                        )}`}
                      >
                        {m.status}
                      </span>
                    </td>
                    <td className="py-2.5 pr-3 font-medium text-white">{m.tenant_name}</td>
                    <td className="py-2.5 pr-3">
                      <div className="text-slate-200">{m.to}</div>
                      <div className="text-[11px] text-slate-400">from {m.from}</div>
                    </td>
                    <td className="py-2.5 pr-3 text-slate-300">
                      {m.encoding} ({m.segments} seg)
                    </td>
                    <td className="py-2.5 pr-3 font-mono text-slate-200">${m.cost}</td>
                    <td className="py-2.5 pr-3 text-slate-400">
                      {m.carrier_used || "-"} ({m.attempt_count} try)
                    </td>
                    <td className="py-2.5 pr-3 text-slate-400">
                      {new Date(m.accepted_at).toLocaleString()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {/* Message Detail Modal */}
      {selectedId && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
          onClick={() => setSelectedId(null)}
        >
          <div
            className="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-xl border border-slate-700 bg-slate-900 p-6 shadow-2xl"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="mb-4 flex items-center justify-between border-b border-slate-800 pb-3">
              <div>
                <h3 className="text-lg font-semibold text-white">Message Audit Detail</h3>
                <p className="font-mono text-xs text-slate-400">{selectedId}</p>
              </div>
              <button
                onClick={() => setSelectedId(null)}
                className="rounded-lg border border-slate-700 px-3 py-1 text-xs text-slate-300 hover:bg-slate-800"
              >
                Close
              </button>
            </div>

            {loadingDetail || !selectedDetail ? (
              <div className="h-32 animate-pulse rounded bg-slate-800" />
            ) : (
              <div className="space-y-5 text-xs">
                <div className="grid grid-cols-2 gap-3 rounded-lg border border-slate-800 bg-slate-950 p-3 sm:grid-cols-3">
                  <div>
                    <span className="text-slate-400">Status</span>
                    <div className="mt-1 font-semibold text-white">{selectedDetail.status}</div>
                  </div>
                  <div>
                    <span className="text-slate-400">Tenant</span>
                    <div className="mt-1 text-white">{selectedDetail.tenant_name}</div>
                  </div>
                  <div>
                    <span className="text-slate-400">To / From</span>
                    <div className="mt-1 text-white">
                      {selectedDetail.to} ({selectedDetail.from})
                    </div>
                  </div>
                  <div>
                    <span className="text-slate-400">Encoding / Segments</span>
                    <div className="mt-1 text-white">
                      {selectedDetail.encoding} · {selectedDetail.units} units ·{" "}
                      {selectedDetail.segments} seg
                    </div>
                  </div>
                  <div>
                    <span className="text-slate-400">Carrier / Carrier ID</span>
                    <div className="mt-1 font-mono text-white">
                      {selectedDetail.carrier_used || "-"} / {selectedDetail.carrier_id || "-"}
                    </div>
                  </div>
                  <div>
                    <span className="text-slate-400">Total Cost</span>
                    <div className="mt-1 font-mono text-emerald-300">${selectedDetail.cost}</div>
                  </div>
                </div>

                <div>
                  <h4 className="mb-1.5 font-semibold text-slate-300">Message Text</h4>
                  <div className="rounded-lg border border-slate-800 bg-slate-950 p-3 text-slate-200">
                    {selectedDetail.text}
                  </div>
                </div>

                <div>
                  <h4 className="mb-2 font-semibold text-slate-300">Event History</h4>
                  <ol className="space-y-2 border-l-2 border-slate-700 pl-4">
                    {selectedDetail.events.map((ev) => (
                      <li key={ev.id} className="relative">
                        <div className="flex items-center justify-between">
                          <span
                            className={`rounded border px-2 py-0.5 text-[11px] font-medium ${statusBadgeClass(
                              ev.status
                            )}`}
                          >
                            {ev.status}
                          </span>
                          <span className="text-[11px] text-slate-400">
                            {new Date(ev.occurred_at).toLocaleString()}
                          </span>
                        </div>
                        {ev.detail && <p className="mt-1 text-slate-300">{ev.detail}</p>}
                      </li>
                    ))}
                  </ol>
                </div>

                <div>
                  <h4 className="mb-2 font-semibold text-slate-300">Ledger Entries</h4>
                  <div className="space-y-1.5">
                    {selectedDetail.ledger_entries.map((le) => (
                      <div
                        key={le.id}
                        className="flex items-center justify-between rounded border border-slate-800 bg-slate-950 px-3 py-2"
                      >
                        <div>
                          <span className="font-semibold text-white">{le.entry_type}</span>{" "}
                          <span className="text-slate-400">— {le.description}</span>
                        </div>
                        <div className="font-mono text-white">
                          amount: {le.amount} | bal: ${le.balance_after}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </main>
  );
}
