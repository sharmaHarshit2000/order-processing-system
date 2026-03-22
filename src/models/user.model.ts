export interface User {
  id: string;
  name: string;
  email: string;
  tier: "free" | "premium" | "vip";
}
