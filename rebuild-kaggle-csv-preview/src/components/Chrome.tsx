import { useState } from "react";
import {
  Bell,
  BookOpen,
  Code2,
  Database,
  Home,
  Menu,
  MessagesSquare,
  MoreHorizontal,
  Plus,
  Search,
  Trophy,
  Boxes,
} from "lucide-react";

export function KaggleLogo() {
  return (
    <div className="flex items-center gap-2 select-none">
      <svg viewBox="0 0 32 32" className="h-6 w-6" aria-hidden>
        <path
          d="M9 2h4v16.6l7.3-7.4h5.1l-7.6 7.5 7.9 11.3h-4.9l-5.6-8.4-2.2 2.1V30H9z"
          fill="#20BEFF"
        />
      </svg>
      <span className="text-[20px] font-bold tracking-tight text-[#20beff]">kaggle</span>
    </div>
  );
}

export function TopNav() {
  return (
    <header className="sticky top-0 z-40 flex h-14 items-center gap-3 border-b border-[#e3e6e8] bg-white px-3">
      <button className="rounded-full p-2 text-[#5f6368] hover:bg-[#f1f3f4]" aria-label="Menu">
        <Menu className="h-5 w-5" />
      </button>
      <KaggleLogo />
      <div className="relative ml-2 hidden max-w-[560px] flex-1 md:block">
        <Search className="pointer-events-none absolute top-1/2 left-3 h-4 w-4 -translate-y-1/2 text-[#5f6368]" />
        <input
          placeholder="Search"
          className="h-10 w-full rounded-lg bg-[#f1f3f4] pr-3 pl-9 text-sm text-[#202124] outline-none placeholder:text-[#5f6368] focus:bg-white focus:ring-1 focus:ring-[#20beff]"
        />
      </div>
      <div className="ml-auto flex items-center gap-1.5">
        <button className="hidden items-center gap-1.5 rounded-full bg-[#20beff] px-3.5 py-2 text-[13px] font-medium text-white hover:bg-[#0f9ad6] sm:flex">
          <Plus className="h-4 w-4" /> Create
        </button>
        <button className="rounded-full p-2 text-[#5f6368] hover:bg-[#f1f3f4]" aria-label="Notifications">
          <Bell className="h-5 w-5" />
        </button>
        <div className="ml-1 grid h-8 w-8 place-items-center rounded-full bg-gradient-to-br from-[#20beff] to-[#0f7fb8] text-[12px] font-semibold text-white">
          MW
        </div>
      </div>
    </header>
  );
}

const NAV = [
  { icon: Home, label: "Home" },
  { icon: Trophy, label: "Competitions" },
  { icon: Database, label: "Datasets", active: true },
  { icon: Boxes, label: "Models" },
  { icon: Code2, label: "Code" },
  { icon: MessagesSquare, label: "Discussions" },
  { icon: BookOpen, label: "Learn" },
  { icon: MoreHorizontal, label: "More" },
];

export function SideNav() {
  const [active, setActive] = useState("Datasets");
  return (
    <nav className="sticky top-14 hidden h-[calc(100vh-3.5rem)] w-[200px] shrink-0 flex-col overflow-y-auto border-r border-[#e3e6e8] bg-white py-3 lg:flex">
      {NAV.map(({ icon: Icon, label }) => {
        const on = active === label;
        return (
          <button
            key={label}
            onClick={() => setActive(label)}
            className={`mr-3 flex items-center gap-4 rounded-r-full py-2.5 pl-6 text-left text-[13px] transition-colors ${
              on ? "bg-[#e8f7fe] font-semibold text-[#0b6c96]" : "text-[#3c4043] hover:bg-[#f1f3f4]"
            }`}
          >
            <Icon className={`h-[18px] w-[18px] ${on ? "text-[#0f9ad6]" : "text-[#5f6368]"}`} />
            {label}
          </button>
        );
      })}
      <div className="mt-4 border-t border-[#e3e6e8] pt-4 pl-6 text-[11px] font-semibold tracking-wide text-[#5f6368] uppercase">
        Recently Viewed
      </div>
      <ul className="mt-1 space-y-0.5 pr-3">
        {["Meta Kaggle", "Kaggle Datasets", "Spotify Tracks", "NYC Airbnb Open Data"].map((t) => (
          <li key={t}>
            <button className="w-full truncate rounded-r-full py-2 pl-6 text-left text-[12.5px] text-[#3c4043] hover:bg-[#f1f3f4]">
              {t}
            </button>
          </li>
        ))}
      </ul>
    </nav>
  );
}
