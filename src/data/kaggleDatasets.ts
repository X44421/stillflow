import type { Row } from "../lib/csv";

/* Column order mirrors the Kaggle public API `dataset_list` payload,
   which is what the kaggle-datasets CSV dump is built from. */
export const CSV_COLUMNS = [
  "id",
  "ref",
  "title",
  "subtitle",
  "creatorName",
  "totalBytes",
  "lastUpdated",
  "downloadCount",
  "viewCount",
  "voteCount",
  "kernelCount",
  "topicCount",
  "currentVersionNumber",
  "usabilityRating",
  "licenseName",
  "tags",
];

/* --------------------------- seeded RNG --------------------------- */
function mulberry32(a: number) {
  return function () {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/* ---------------------- real-looking seed rows -------------------- */
const REAL: [string, string, string, string][] = [
  ["kaggle/meta-kaggle", "Meta Kaggle", "Kaggle's public data on competitions, users, submission scores, and kernels", "Kaggle"],
  ["zynicide/wine-reviews", "Wine Reviews", "130k wine reviews with variety, location, winery, price and description", "zackthoutt"],
  ["uciml/iris", "Iris Species", "Classify iris plants into three species in this classic dataset", "UCI Machine Learning"],
  ["mlg-ulb/creditcardfraud", "Credit Card Fraud Detection", "Anonymized credit card transactions labeled as fraudulent or genuine", "Machine Learning Group - ULB"],
  ["shivamb/netflix-shows", "Netflix Movies and TV Shows", "Listings of movies and TV shows on Netflix - Regularly Updated", "Shivam Bansal"],
  ["rtatman/188-million-us-wildfires", "1.88 Million US Wildfires", "24 years of geo-referenced wildfire records", "Rachael Tatman"],
  ["c/titanic", "Titanic - Machine Learning from Disaster", "Start here! Predict survival on the Titanic", "Kaggle"],
  ["sudalairajkumar/novel-corona-virus-2019-dataset", "COVID-19 Dataset", "Day level information on covid-19 affected cases", "SRK"],
  ["gregorut/videogamesales", "Video Game Sales", "Analyze sales data from more than 16,500 games", "GregorySmith"],
  ["arshid/iris-flower-dataset", "Iris Flower Dataset", "Iris flower data set used for multi-class classification", "Arshid"],
  ["olistbr/brazilian-ecommerce", "Brazilian E-Commerce Public Dataset by Olist", "100,000 orders from 2016 to 2018 made at multiple marketplaces", "Olist"],
  ["datasnaek/youtube-new", "Trending YouTube Video Statistics", "Daily statistics for trending YouTube videos", "Mitchell J"],
  ["stackoverflow/stack-overflow-2018-developer-survey", "Stack Overflow 2018 Developer Survey", "The largest and most comprehensive developer survey", "Stack Overflow"],
  ["nasa/kepler-exoplanet-search-results", "Kepler Exoplanet Search Results", "10000 exoplanet candidates examined by the Kepler Space Observatory", "NASA"],
  ["ronitf/heart-disease-uci", "Heart Disease UCI", "Predict the presence of heart disease in the patient", "ronitf"],
  ["blastchar/telco-customer-churn", "Telco Customer Churn", "Focused customer retention programs", "BlastChar"],
  ["camnugent/california-housing-prices", "California Housing Prices", "Median house prices for California districts from the 1990 census", "Cam Nugent"],
  ["russellyates88/suicide-rates-overview-1985-to-2016", "Suicide Rates Overview 1985 to 2016", "Compares socio-economic info with suicide rates by year and country", "Rusty"],
  ["unsdsn/world-happiness", "World Happiness Report", "Happiness scored according to economic production, social support, etc.", "Sustainable Development Solutions Network"],
  ["START-UMD/gtd", "Global Terrorism Database", "More than 180,000 terrorist attacks worldwide, 1970-2017", "START Consortium"],
  ["crawford/80-cereals", "80 Cereals", "Nutrition data on 80 cereal products", "Chris Crawford"],
  ["aaron7sun/stocknews", "Daily News for Stock Market Prediction", "Using 8 years of daily news headlines to predict stock market movement", "Aaron7sun"],
  ["kazanova/sentiment140", "Sentiment140 dataset with 1.6 million tweets", "Sentiment analysis with tweets", "kazanova"],
  ["snap/amazon-fine-food-reviews", "Amazon Fine Food Reviews", "Analyze ~500,000 food reviews from Amazon", "Stanford Network Analysis Project"],
  ["moltean/fruits", "Fruits 360", "A dataset with 90483 images of 131 fruits and vegetables", "Mihai Oltean"],
  ["paultimothymooney/chest-xray-pneumonia", "Chest X-Ray Images (Pneumonia)", "5,863 images, 2 categories", "Paul Mooney"],
  ["andrewmvd/face-mask-detection", "Face Mask Detection", "853 images belonging to 3 classes", "Larxel"],
  ["ashishpatel26/wm811k-wafer-map", "WM-811K wafer map", "Wafer map failure pattern recognition", "Ashish Patel"],
  ["heptapod/titanic", "Titanic Cleaned Data", "A cleaned version of the classic Titanic dataset", "heptapod"],
  ["sohier/calcofi", "CalCOFI", "Over 60 years of oceanographic data", "Sohier Dane"],
  ["berkeleyearth/climate-change-earth-surface-temperature-data", "Climate Change: Earth Surface Temperature Data", "Exploring global temperatures since 1750", "Berkeley Earth"],
  ["kemical/kickstarter-projects", "Kickstarter Projects", "More than 300,000 kickstarter projects", "Mickaël Mouillé"],
  ["carrie1/ecommerce-data", "E-Commerce Data", "Actual transactions from UK retailer", "Carrie1"],
  ["dgomonov/new-york-city-airbnb-open-data", "New York City Airbnb Open Data", "Airbnb listings and metrics in NYC, NY, USA (2019)", "Dgomonov"],
  ["shrutimehta/zomato-restaurants-data", "Zomato Restaurants Data", "Restaurant details from the Zomato API", "Shruti Mehta"],
  ["rush4ratio/video-game-sales-with-ratings", "Video Game Sales with Ratings", "Video game sales combined with Metacritic ratings", "Rush Kirubi"],
  ["fedesoriano/stroke-prediction-dataset", "Stroke Prediction Dataset", "11 clinical features for predicting stroke events", "fedesoriano"],
  ["mathchi/diabetes-data-set", "Diabetes Dataset", "The Pima Indians Diabetes Database", "Mehmet Akturk"],
  ["yasserh/housing-prices-dataset", "Housing Prices Dataset", "Predict the price of a house using its features", "M Yasser H"],
  ["arnabchaki/data-science-salaries-2023", "Data Science Salaries 2023", "Salaries of different data science fields in the data science domain", "randomarnab"],
  ["nelgiriyewithana/top-spotify-songs-2023", "Most Streamed Spotify Songs 2023", "Track features, popularity and presence in playlists", "Nidula Elgiriyewithana"],
  ["whenamancodes/popular-movies-datasets-58000-movies", "Popular Movies Dataset", "58,000 movies with genre, rating and revenue", "Aman Chauhan"],
  ["thedevastator/global-video-game-sales", "Global Video Game Sales", "Sales across regions, platforms and publishers", "The Devastator"],
  ["iamsouravbanerjee/world-population-dataset", "World Population Dataset", "Population of every country from 1970 to 2022", "Sourav Banerjee"],
  ["anthonytherrien/depression-dataset", "Depression Dataset", "Synthetic dataset for mental health analysis", "Anthony Therrien"],
  ["computingvictor/transactions-fraud-datasets", "Financial Transactions Dataset", "Analytics for fraud detection on card transactions", "Victor Ruiz"],
  ["hummaamqaasim/jobs-in-data", "Jobs and Salaries in Data Science", "Compensation across roles, seniority and geography", "Hummaam Qaasim"],
  ["joebeachcapital/students-performance", "Students Performance in Exams", "Marks secured by students in various subjects", "Joakim Arvidsson"],
];

const OWNERS = [
  "Kaggle", "UCI Machine Learning", "Larxel", "fedesoriano", "The Devastator",
  "Shivam Bansal", "Rohit Sahoo", "Sourav Banerjee", "Paul Mooney", "Mitchell J",
  "Ruchi Bhatia", "Ashish Patel", "Nidula Elgiriyewithana", "Sarah Jeffreson",
  "Open Data Society", "World Bank Group", "NASA", "Google BigQuery", "SRK",
  "Anthony Therrien", "Joakim Arvidsson", "M Yasser H", "Aman Chauhan",
];

const TOPICS = [
  "Global Air Quality", "Retail Sales", "Bitcoin Historical", "Netflix Titles",
  "Student Performance", "Heart Failure Clinical", "Airline Passenger Satisfaction",
  "Spotify Tracks", "Amazon Product Reviews", "House Rent", "Solar Power Generation",
  "Crop Production", "Road Accidents", "Bank Marketing", "Employee Attrition",
  "Fake News", "Handwritten Digits", "Chess Games", "Olympic History",
  "World University Rankings", "Used Car Listings", "Flight Delays",
  "Electric Vehicle Population", "Mental Health in Tech", "Supermarket Basket",
  "Cybersecurity Attacks", "IMDb Movie", "Weather History", "Football Matches",
  "Cryptocurrency Prices", "E-Sports Earnings", "Traffic Volume", "Sleep Health",
  "Diabetes Health Indicators", "Customer Personality", "Loan Default",
  "Hotel Bookings", "Wildfire Perimeters", "Air Pollution PM2.5", "Coffee Quality",
];

const SUFFIX = [
  "Dataset", "Data 2024", "Analytics", "Time Series", "2015-2024", "Clean Version",
  "EDA Ready", "Full Archive", "Sample", "Extended", "with Images", "Daily Updated",
];

const TAG_POOL = [
  "business", "health", "education", "finance", "computer science", "internet",
  "classification", "regression", "nlp", "computer vision", "time series",
  "beginner", "data visualization", "tabular", "exploratory data analysis",
  "sports", "earth and nature", "government", "travel", "energy", "music",
];

const LICENSES = [
  "CC0: Public Domain", "CC0: Public Domain", "CC0: Public Domain",
  "CC BY-SA 4.0", "CC BY-SA 4.0",
  "Other (specified in description)",
  "Database: Open Database, Contents: Database Contents",
  "GPL 2", "Unknown", "Attribution 4.0 International (CC BY 4.0)",
];

function slug(s: string) {
  return s.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, "");
}

function pick<T>(r: () => number, arr: T[]): T {
  return arr[Math.floor(r() * arr.length)];
}

function powerInt(r: () => number, scale: number, max: number) {
  const v = Math.floor(Math.exp(r() * Math.log(scale)) * (0.4 + r()));
  return Math.min(max, Math.max(0, v));
}

function isoDate(r: () => number) {
  const start = Date.UTC(2016, 0, 1);
  const end = Date.UTC(2025, 10, 20);
  // skew toward recent dates
  const t = start + (end - start) * Math.pow(r(), 0.55);
  const d = new Date(t);
  const p = (n: number) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}`;
}

const QUALIFIER = [
  "USA", "India", "Europe", "Global", "Kaggle Edition", "v2", "Regional",
  "2019-2023", "Weekly", "Raw", "Curated", "by City", "by Country", "Mini",
];

export function buildRows(count = 1000): Row[] {
  const r = mulberry32(20240426);
  const rows: Row[] = [];
  const seenTitle = new Set<string>();
  const seenRef = new Set<string>();

  for (let i = 0; i < count; i++) {
    const real = i < REAL.length ? REAL[i] : null;
    const owner = real ? real[3] : pick(r, OWNERS);
    const topic = pick(r, TOPICS);
    let title = real ? real[1] : `${topic} ${pick(r, SUFFIX)}`;
    let guard = 0;
    while (!real && seenTitle.has(title) && guard++ < 6) title = `${topic} ${pick(r, QUALIFIER)} ${pick(r, SUFFIX)}`;
    seenTitle.add(title);
    let ref = real ? real[0] : `${slug(owner)}/${slug(title)}`;
    if (seenRef.has(ref)) ref = `${ref}-${i}`;
    seenRef.add(ref);
    const votes = powerInt(r, 4000, 21000);
    const downloads = Math.floor(votes * (18 + r() * 70) + r() * 900);
    const views = Math.floor(downloads * (4 + r() * 6));
    const tagCount = 2 + Math.floor(r() * 3);
    const tags: string[] = [];
    while (tags.length < tagCount) {
      const t = pick(r, TAG_POOL);
      if (!tags.includes(t)) tags.push(t);
    }

    // a handful of deliberately dirty cells — Kaggle surfaces these as
    // "Mismatched" (red) and "Missing" (grey) in the column summary bar.
    const dirtyVote = !real && r() < 0.012;
    const noSubtitle = !real && r() < 0.17;
    const noVersion = !real && r() < 0.03;

    rows.push({
      id: String(1000 + i * 37 + Math.floor(r() * 30)),
      ref,
      title,
      subtitle: noSubtitle ? "" : real ? real[2] : `${topic} records collected for analysis and modelling`,
      creatorName: owner,
      totalBytes: String(Math.floor(Math.exp(r() * 16) * 900) + 1024),
      lastUpdated: isoDate(r),
      downloadCount: String(downloads),
      viewCount: String(views),
      voteCount: dirtyVote ? "unknown" : String(votes),
      kernelCount: String(powerInt(r, 700, 4200)),
      topicCount: String(powerInt(r, 60, 400)),
      currentVersionNumber: noVersion ? "" : String(1 + Math.floor(Math.pow(r(), 2.2) * 40)),
      usabilityRating: (Math.round((0.35 + Math.pow(r(), 0.55) * 0.65) * 100) / 100).toFixed(2),
      licenseName: pick(r, LICENSES),
      tags: tags.join("|"),
    });
  }
  return rows;
}

export const FILE_META = {
  name: "kaggle_datasets.csv",
  sizeLabel: "38.4 MB",
  rowsLabel: "1000",
  columns: CSV_COLUMNS.length,
};
