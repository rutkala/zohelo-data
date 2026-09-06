interface PlatformInfoLinksProps {
  className?: string;
}

const getInfoUrl = (page: string) => {
  const baseUrl = import.meta.env.BASE_URL === "./" ? "/" : (import.meta.env.BASE_URL ?? "/");
  return `${baseUrl.replace(/\/$/, "")}/${page}`;
};

export default function PlatformInfoLinks({ className = "" }: PlatformInfoLinksProps) {
  return (
    <nav
      aria-label="Platform information"
      className={`flex flex-wrap justify-center gap-x-3 gap-y-1 ${className}`}
    >
      <a
        href={getInfoUrl("about.html")}
        target="_blank"
        rel="noopener noreferrer"
        className="underline underline-offset-2 hover:text-foreground"
      >
        About Zohelo-data
      </a>
      <a
        href={getInfoUrl("privacy.html")}
        target="_blank"
        rel="noopener noreferrer"
        className="underline underline-offset-2 hover:text-foreground"
      >
        Privacy
      </a>
      <a
        href={getInfoUrl("terms.html")}
        target="_blank"
        rel="noopener noreferrer"
        className="underline underline-offset-2 hover:text-foreground"
      >
        Terms
      </a>
    </nav>
  );
}
