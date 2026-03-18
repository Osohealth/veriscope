import { useQuery } from "@tanstack/react-query";
import { getQueryFn } from "@/lib/queryClient";

interface CurrentUser {
    id: string;
    email: string;
    role: string;
}

export function useCurrentUser() {
    return useQuery<CurrentUser | null>({
        queryKey: ["/api/auth/me"],
        queryFn: getQueryFn({ on401: "returnNull" }),
        staleTime: 5 * 60 * 1000, // 5 minutes
        retry: false,
    });
}
