import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "DCC Mini SMS Gateway — Operations Dashboard",
  description: "Real-time multi-tenant SMS gateway monitoring, ledger & test dispatch",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="bg-slate-950 text-slate-100 antialiased">{children}</body>
    </html>
  );
}
