import { useState } from "react";
import { ChevronLeft, ChevronRight, Maximize2, MoreVertical, Plus } from "lucide-react";
import { cn } from "../utils/cn";
import { previewRows } from "../data";

const columns = [
  { name: "order_id", type: "int64" },
  { name: "order_date", type: "date" },
  { name: "customer_id", type: "int64" },
  { name: "country", type: "string" },
  { name: "product_category", type: "string" },
  { name: "sales_amount", type: "float64" },
];

export default function PreviewTable() {
  const [page, setPage] = useState(1);
  const totalPages = 253;

  return (
    <div className="flex h-full flex-col rounded-t-xl border border-zinc-200 bg-white shadow-[0_-4px_16px_rgba(0,0,0,0.04)]">
      {/* Header */}
      <div className="flex shrink-0 items-center justify-between border-b border-zinc-200 px-4 py-2.5">
        <h3 className="text-sm font-semibold text-zinc-900">Clean Sales Preview</h3>
        <div className="flex items-center gap-2">
          <span className="text-[13px] text-zinc-500">12,614 rows · 24 columns</span>
          <button className="rounded-md p-1.5 text-zinc-500 hover:bg-zinc-100">
            <Maximize2 className="h-3.5 w-3.5" />
          </button>
          <button className="rounded-md p-1.5 text-zinc-500 hover:bg-zinc-100">
            <MoreVertical className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="min-h-0 flex-1 overflow-auto">
        <table className="w-full border-collapse text-[13px]">
          <thead className="sticky top-0 z-10">
            <tr className="bg-zinc-50 text-left">
              <th className="w-10 border-b border-r border-zinc-200 px-3 py-2 font-medium text-zinc-500">
                #
              </th>
              {columns.map((col) => (
                <th key={col.name} className="border-b border-r border-zinc-200 px-3 py-1.5">
                  <span className="block font-semibold text-zinc-800">{col.name}</span>
                  <span className="block text-[11px] font-normal text-zinc-400">{col.type}</span>
                </th>
              ))}
              <th className="w-10 border-b border-zinc-200 px-2 py-2">
                <button className="flex h-6 w-6 items-center justify-center rounded-md text-zinc-400 hover:bg-zinc-100 hover:text-zinc-700">
                  <Plus className="h-4 w-4" />
                </button>
              </th>
            </tr>
          </thead>
          <tbody>
            {previewRows.map((row, i) => (
              <tr key={row.orderId} className="hover:bg-zinc-50">
                <td className="border-b border-r border-zinc-100 px-3 py-2 text-zinc-400">
                  {i + 1}
                </td>
                <td className="border-b border-r border-zinc-100 px-3 py-2 text-zinc-800">
                  {row.orderId}
                </td>
                <td className="border-b border-r border-zinc-100 px-3 py-2 text-zinc-800">
                  {row.orderDate}
                </td>
                <td className="border-b border-r border-zinc-100 px-3 py-2 text-zinc-800">
                  {row.customerId}
                </td>
                <td className="border-b border-r border-zinc-100 px-3 py-2 text-zinc-800">
                  {row.country}
                </td>
                <td className="border-b border-r border-zinc-100 px-3 py-2 text-zinc-800">
                  {row.category}
                </td>
                <td className="border-b border-r border-zinc-100 px-3 py-2 text-zinc-800">
                  {row.amount}
                </td>
                <td className="border-b border-zinc-100" />
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div className="flex shrink-0 items-center justify-between border-t border-zinc-200 px-4 py-2">
        <span className="text-[13px] text-zinc-500">Showing 1 to 50 of 12,614 rows</span>
        <div className="flex items-center gap-1">
          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            className="flex h-7 w-7 items-center justify-center rounded-md text-zinc-500 hover:bg-zinc-100 disabled:opacity-40"
            disabled={page === 1}
          >
            <ChevronLeft className="h-4 w-4" />
          </button>
          {[1, 2, 3].map((p) => (
            <button
              key={p}
              onClick={() => setPage(p)}
              className={cn(
                "h-7 min-w-7 rounded-md px-1.5 text-[13px]",
                page === p
                  ? "bg-zinc-900 font-medium text-white"
                  : "text-zinc-600 hover:bg-zinc-100"
              )}
            >
              {p}
            </button>
          ))}
          <span className="px-1 text-[13px] text-zinc-400">…</span>
          <button
            onClick={() => setPage(totalPages)}
            className={cn(
              "h-7 min-w-7 rounded-md px-1.5 text-[13px]",
              page === totalPages
                ? "bg-zinc-900 font-medium text-white"
                : "text-zinc-600 hover:bg-zinc-100"
            )}
          >
            {totalPages}
          </button>
          <button
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            className="flex h-7 w-7 items-center justify-center rounded-md text-zinc-500 hover:bg-zinc-100 disabled:opacity-40"
            disabled={page === totalPages}
          >
            <ChevronRight className="h-4 w-4" />
          </button>
        </div>
      </div>
    </div>
  );
}
