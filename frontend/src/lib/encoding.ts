export type EncodingName = "GSM7" | "UCS2";

const GSM7_BASIC = new Set(
  "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞ" +
    "ÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?" +
    "¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§" +
    "¿abcdefghijklmnopqrstuvwxyzäöñüà"
);

const GSM7_EXT = new Set("^{}\\[~]|€\f");

export interface EncodingEstimate {
  encoding: EncodingName;
  units: number;
  segments: number;
  singleLimit: number;
  multiLimit: number;
}

export function analyzeEncoding(text: string): EncodingEstimate {
  if (!text) {
    return { encoding: "GSM7", units: 0, segments: 0, singleLimit: 160, multiLimit: 153 };
  }

  let isGsm7 = true;
  let septets = 0;

  for (const ch of text) {
    if (GSM7_BASIC.has(ch)) {
      septets += 1;
    } else if (GSM7_EXT.has(ch)) {
      septets += 2;
    } else {
      isGsm7 = false;
      break;
    }
  }

  if (isGsm7) {
    const segments = septets <= 160 ? 1 : Math.ceil(septets / 153);
    return { encoding: "GSM7", units: septets, segments, singleLimit: 160, multiLimit: 153 };
  }

  // JavaScript string.length counts UTF-16 code units, matching Python utf-16-be // 2
  const units = text.length;
  const segments = units <= 70 ? 1 : Math.ceil(units / 67);
  return { encoding: "UCS2", units, segments, singleLimit: 70, multiLimit: 67 };
}

export interface PriceEntry {
  prefix: string;
  country_name: string;
  price_per_segment: string;
}

export function estimatePrice(
  toNumber: string,
  segments: number,
  prices: PriceEntry[]
): { prefix: string; country: string; unitPrice: number; total: number } | null {
  if (!/^\+[1-9]\d{6,14}$/.test(toNumber) || segments <= 0) {
    return null;
  }
  const sorted = [...prices].sort((a, b) => b.prefix.length - a.prefix.length);
  for (const p of sorted) {
    if (toNumber.startsWith(p.prefix)) {
      const unitPrice = Number(p.price_per_segment);
      return {
        prefix: p.prefix,
        country: p.country_name,
        unitPrice,
        total: unitPrice * segments,
      };
    }
  }
  return null;
}
