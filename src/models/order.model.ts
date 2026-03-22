export type OrderStatus =
  | "pending"
  | "confirmed"
  | "payment_processing"
  | "paid"
  | "fulfilling"
  | "shipped"
  | "delivered"
  | "cancelled"
  | "failed";

export interface OrderItem {
  productId: string;
  productName: string;
  quantity: number;
  unitPrice: number;
  totalPrice: number;
}

export interface Order {
  id: string;
  userId: string;
  items: OrderItem[];
  totalAmount: number;
  status: OrderStatus;
  createdAt: string;
  updatedAt: string;
}
