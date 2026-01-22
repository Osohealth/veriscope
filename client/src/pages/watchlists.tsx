import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger, DialogFooter } from "@/components/ui/dialog";
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from "@/components/ui/alert-dialog";
import { useToast } from "@/hooks/use-toast";
import { queryClient, apiRequest } from "@/lib/queryClient";
import { Ship, Anchor, Droplets, Plus, Trash2, Edit2, Eye, Star } from "lucide-react";
import type { Watchlist } from "@shared/schema";

export default function WatchlistsPage() {
  const { toast } = useToast();
  const [isCreateOpen, setIsCreateOpen] = useState(false);
  const [editingWatchlist, setEditingWatchlist] = useState<Watchlist | null>(null);
  const [formData, setFormData] = useState({
    name: "",
    type: "vessels",
    items: [] as string[],
    itemInput: ""
  });

  const { data: watchlists = [], isLoading } = useQuery<Watchlist[]>({
    queryKey: ["/api/watchlists"]
  });

  const { data: vessels = [] } = useQuery<any[]>({
    queryKey: ["/api/vessels"]
  });

  const { data: ports = [] } = useQuery<any[]>({
    queryKey: ["/api/ports"]
  });

  const createMutation = useMutation({
    mutationFn: async (data: { name: string; type: string; items: any }) => {
      return await apiRequest("POST", "/api/watchlists", data);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/watchlists"] });
      setIsCreateOpen(false);
      resetForm();
      toast({ title: "Watchlist created successfully" });
    },
    onError: () => {
      toast({ title: "Failed to create watchlist", variant: "destructive" });
    }
  });

  const updateMutation = useMutation({
    mutationFn: async ({ id, data }: { id: string; data: any }) => {
      return await apiRequest("PATCH", `/api/watchlists/${id}`, data);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/watchlists"] });
      setEditingWatchlist(null);
      resetForm();
      toast({ title: "Watchlist updated successfully" });
    },
    onError: () => {
      toast({ title: "Failed to update watchlist", variant: "destructive" });
    }
  });

  const deleteMutation = useMutation({
    mutationFn: async (id: string) => {
      return await apiRequest("DELETE", `/api/watchlists/${id}`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/watchlists"] });
      toast({ title: "Watchlist deleted successfully" });
    },
    onError: () => {
      toast({ title: "Failed to delete watchlist", variant: "destructive" });
    }
  });

  const resetForm = () => {
    setFormData({ name: "", type: "vessels", items: [], itemInput: "" });
  };

  const handleAddItem = () => {
    if (formData.itemInput && !formData.items.includes(formData.itemInput)) {
      setFormData({
        ...formData,
        items: [...formData.items, formData.itemInput],
        itemInput: ""
      });
    }
  };

  const handleRemoveItem = (item: string) => {
    setFormData({
      ...formData,
      items: formData.items.filter((i) => i !== item)
    });
  };

  const handleSubmit = () => {
    if (editingWatchlist) {
      updateMutation.mutate({
        id: editingWatchlist.id,
        data: {
          name: formData.name,
          items: formData.items
        }
      });
    } else {
      createMutation.mutate({
        name: formData.name,
        type: formData.type,
        items: formData.items
      });
    }
  };

  const openEdit = (watchlist: Watchlist) => {
    setEditingWatchlist(watchlist);
    setFormData({
      name: watchlist.name,
      type: watchlist.type,
      items: Array.isArray(watchlist.items) ? watchlist.items : [],
      itemInput: ""
    });
  };

  const getTypeIcon = (type: string) => {
    switch (type) {
      case "vessels":
        return <Ship className="h-5 w-5" />;
      case "ports":
        return <Anchor className="h-5 w-5" />;
      case "commodities":
        return <Droplets className="h-5 w-5" />;
      default:
        return <Eye className="h-5 w-5" />;
    }
  };

  const getItemLabel = (type: string, itemId: string) => {
    if (type === "vessels") {
      const vessel = vessels.find((v: any) => v.id === itemId || v.mmsi === itemId);
      return vessel?.name || itemId;
    }
    if (type === "ports") {
      const port = ports.find((p: any) => p.id === itemId || p.code === itemId);
      return port?.name || itemId;
    }
    return itemId;
  };

  return (
    <div className="p-6 space-y-6 bg-slate-950 min-h-screen">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-white">Watchlists</h1>
          <p className="text-slate-400 mt-1">Track vessels, ports, and commodities of interest</p>
        </div>
        <Dialog open={isCreateOpen} onOpenChange={setIsCreateOpen}>
          <DialogTrigger asChild>
            <Button data-testid="button-create-watchlist" className="bg-blue-600 hover:bg-blue-700">
              <Plus className="h-4 w-4 mr-2" />
              Create Watchlist
            </Button>
          </DialogTrigger>
          <DialogContent className="bg-slate-900 border-slate-700">
            <DialogHeader>
              <DialogTitle className="text-white">Create New Watchlist</DialogTitle>
            </DialogHeader>
            <div className="space-y-4 py-4">
              <div className="space-y-2">
                <Label className="text-slate-300">Name</Label>
                <Input
                  data-testid="input-watchlist-name"
                  value={formData.name}
                  onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                  placeholder="My Watchlist"
                  className="bg-slate-800 border-slate-700 text-white"
                />
              </div>
              <div className="space-y-2">
                <Label className="text-slate-300">Type</Label>
                <Select
                  value={formData.type}
                  onValueChange={(value) => setFormData({ ...formData, type: value, items: [] })}
                >
                  <SelectTrigger data-testid="select-watchlist-type" className="bg-slate-800 border-slate-700 text-white">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent className="bg-slate-800 border-slate-700">
                    <SelectItem value="vessels">Vessels</SelectItem>
                    <SelectItem value="ports">Ports</SelectItem>
                    <SelectItem value="commodities">Commodities</SelectItem>
                    <SelectItem value="routes">Routes</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label className="text-slate-300">Add Items</Label>
                <div className="flex gap-2">
                  {formData.type === "vessels" ? (
                    <Select
                      value={formData.itemInput}
                      onValueChange={(value) => setFormData({ ...formData, itemInput: value })}
                    >
                      <SelectTrigger className="bg-slate-800 border-slate-700 text-white flex-1">
                        <SelectValue placeholder="Select a vessel" />
                      </SelectTrigger>
                      <SelectContent className="bg-slate-800 border-slate-700">
                        {vessels.map((v: any) => (
                          <SelectItem key={v.id} value={v.id}>{v.name}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  ) : formData.type === "ports" ? (
                    <Select
                      value={formData.itemInput}
                      onValueChange={(value) => setFormData({ ...formData, itemInput: value })}
                    >
                      <SelectTrigger className="bg-slate-800 border-slate-700 text-white flex-1">
                        <SelectValue placeholder="Select a port" />
                      </SelectTrigger>
                      <SelectContent className="bg-slate-800 border-slate-700">
                        {ports.map((p: any) => (
                          <SelectItem key={p.id} value={p.id}>{p.name}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  ) : (
                    <Input
                      value={formData.itemInput}
                      onChange={(e) => setFormData({ ...formData, itemInput: e.target.value })}
                      placeholder="Enter ID or name"
                      className="bg-slate-800 border-slate-700 text-white flex-1"
                    />
                  )}
                  <Button 
                    data-testid="button-add-item"
                    onClick={handleAddItem} 
                    variant="outline" 
                    className="border-slate-700"
                  >
                    Add
                  </Button>
                </div>
                <div className="flex flex-wrap gap-2 mt-2">
                  {formData.items.map((item) => (
                    <Badge key={item} variant="secondary" className="bg-slate-700">
                      {getItemLabel(formData.type, item)}
                      <button
                        onClick={() => handleRemoveItem(item)}
                        className="ml-2 hover:text-red-400"
                      >
                        ×
                      </button>
                    </Badge>
                  ))}
                </div>
              </div>
            </div>
            <DialogFooter>
              <Button
                data-testid="button-save-watchlist"
                onClick={handleSubmit}
                disabled={!formData.name || formData.items.length === 0 || createMutation.isPending}
                className="bg-blue-600 hover:bg-blue-700"
              >
                {createMutation.isPending ? "Creating..." : "Create Watchlist"}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>

      {isLoading ? (
        <div className="text-slate-400">Loading watchlists...</div>
      ) : watchlists.length === 0 ? (
        <Card className="bg-slate-900 border-slate-800">
          <CardContent className="p-12 text-center">
            <Star className="h-12 w-12 mx-auto text-slate-600 mb-4" />
            <h3 className="text-xl font-semibold text-white mb-2">No Watchlists Yet</h3>
            <p className="text-slate-400 mb-4">Create your first watchlist to start tracking vessels, ports, or commodities</p>
            <Button 
              data-testid="button-create-first-watchlist"
              onClick={() => setIsCreateOpen(true)} 
              className="bg-blue-600 hover:bg-blue-700"
            >
              Create Your First Watchlist
            </Button>
          </CardContent>
        </Card>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {watchlists.map((watchlist) => (
            <Card key={watchlist.id} data-testid={`card-watchlist-${watchlist.id}`} className="bg-slate-900 border-slate-800 hover:border-slate-700 transition-colors">
              <CardHeader className="pb-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <div className="p-2 bg-slate-800 rounded-lg text-blue-400">
                      {getTypeIcon(watchlist.type)}
                    </div>
                    <div>
                      <CardTitle className="text-white text-lg">{watchlist.name}</CardTitle>
                      <CardDescription className="text-slate-400 capitalize">{watchlist.type}</CardDescription>
                    </div>
                  </div>
                  {watchlist.isDefault && (
                    <Badge className="bg-amber-600">Default</Badge>
                  )}
                </div>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="flex flex-wrap gap-2">
                  {Array.isArray(watchlist.items) && watchlist.items.slice(0, 5).map((item: string) => (
                    <Badge key={item} variant="outline" className="border-slate-700 text-slate-300">
                      {getItemLabel(watchlist.type, item)}
                    </Badge>
                  ))}
                  {Array.isArray(watchlist.items) && watchlist.items.length > 5 && (
                    <Badge variant="outline" className="border-slate-700 text-slate-400">
                      +{watchlist.items.length - 5} more
                    </Badge>
                  )}
                </div>
                <div className="flex justify-end gap-2">
                  <Button
                    data-testid={`button-edit-${watchlist.id}`}
                    variant="ghost"
                    size="sm"
                    onClick={() => openEdit(watchlist)}
                    className="text-slate-400 hover:text-white"
                  >
                    <Edit2 className="h-4 w-4" />
                  </Button>
                  <AlertDialog>
                    <AlertDialogTrigger asChild>
                      <Button
                        data-testid={`button-delete-${watchlist.id}`}
                        variant="ghost"
                        size="sm"
                        className="text-slate-400 hover:text-red-400"
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </AlertDialogTrigger>
                    <AlertDialogContent className="bg-slate-900 border-slate-700">
                      <AlertDialogHeader>
                        <AlertDialogTitle className="text-white">Delete Watchlist</AlertDialogTitle>
                        <AlertDialogDescription className="text-slate-400">
                          Are you sure you want to delete "{watchlist.name}"? This action cannot be undone.
                        </AlertDialogDescription>
                      </AlertDialogHeader>
                      <AlertDialogFooter>
                        <AlertDialogCancel className="bg-slate-800 border-slate-700 text-white">Cancel</AlertDialogCancel>
                        <AlertDialogAction
                          data-testid="button-confirm-delete"
                          onClick={() => deleteMutation.mutate(watchlist.id)}
                          className="bg-red-600 hover:bg-red-700"
                        >
                          Delete
                        </AlertDialogAction>
                      </AlertDialogFooter>
                    </AlertDialogContent>
                  </AlertDialog>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      <Dialog open={!!editingWatchlist} onOpenChange={(open) => !open && setEditingWatchlist(null)}>
        <DialogContent className="bg-slate-900 border-slate-700">
          <DialogHeader>
            <DialogTitle className="text-white">Edit Watchlist</DialogTitle>
          </DialogHeader>
          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <Label className="text-slate-300">Name</Label>
              <Input
                data-testid="input-edit-name"
                value={formData.name}
                onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                className="bg-slate-800 border-slate-700 text-white"
              />
            </div>
            <div className="space-y-2">
              <Label className="text-slate-300">Items</Label>
              <div className="flex gap-2">
                {editingWatchlist?.type === "vessels" ? (
                  <Select
                    value={formData.itemInput}
                    onValueChange={(value) => setFormData({ ...formData, itemInput: value })}
                  >
                    <SelectTrigger className="bg-slate-800 border-slate-700 text-white flex-1">
                      <SelectValue placeholder="Select a vessel" />
                    </SelectTrigger>
                    <SelectContent className="bg-slate-800 border-slate-700">
                      {vessels.map((v: any) => (
                        <SelectItem key={v.id} value={v.id}>{v.name}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                ) : editingWatchlist?.type === "ports" ? (
                  <Select
                    value={formData.itemInput}
                    onValueChange={(value) => setFormData({ ...formData, itemInput: value })}
                  >
                    <SelectTrigger className="bg-slate-800 border-slate-700 text-white flex-1">
                      <SelectValue placeholder="Select a port" />
                    </SelectTrigger>
                    <SelectContent className="bg-slate-800 border-slate-700">
                      {ports.map((p: any) => (
                        <SelectItem key={p.id} value={p.id}>{p.name}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                ) : (
                  <Input
                    value={formData.itemInput}
                    onChange={(e) => setFormData({ ...formData, itemInput: e.target.value })}
                    placeholder="Enter ID"
                    className="bg-slate-800 border-slate-700 text-white flex-1"
                  />
                )}
                <Button onClick={handleAddItem} variant="outline" className="border-slate-700">
                  Add
                </Button>
              </div>
              <div className="flex flex-wrap gap-2 mt-2">
                {formData.items.map((item) => (
                  <Badge key={item} variant="secondary" className="bg-slate-700">
                    {getItemLabel(editingWatchlist?.type || "vessels", item)}
                    <button
                      onClick={() => handleRemoveItem(item)}
                      className="ml-2 hover:text-red-400"
                    >
                      ×
                    </button>
                  </Badge>
                ))}
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button
              data-testid="button-update-watchlist"
              onClick={handleSubmit}
              disabled={!formData.name || updateMutation.isPending}
              className="bg-blue-600 hover:bg-blue-700"
            >
              {updateMutation.isPending ? "Saving..." : "Save Changes"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
