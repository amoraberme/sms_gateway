export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(request: Request) {
  const backendUrl = process.env.BACKEND_INTERNAL_URL || "http://127.0.0.1:8000";
  const controller = new AbortController();

  request.signal.addEventListener("abort", () => {
    controller.abort();
  });

  const upstream = await fetch(`${backendUrl}/internal/dashboard/events`, {
    method: "GET",
    headers: { Accept: "text/event-stream" },
    cache: "no-store",
    signal: controller.signal,
  });

  if (!upstream.ok || !upstream.body) {
    return new Response("Upstream SSE unavailable", { status: upstream.status || 502 });
  }

  return new Response(upstream.body, {
    status: 200,
    headers: {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    },
  });
}
