output "cluster_id" {
  description = "UUID of the DOKS cluster"
  value       = digitalocean_kubernetes_cluster.tether.id
}

output "cluster_name" {
  description = "Name of the DOKS cluster, used for doctl kubeconfig save"
  value       = digitalocean_kubernetes_cluster.tether.name
}

output "cluster_endpoint" {
  description = "API server endpoint of the cluster"
  value       = digitalocean_kubernetes_cluster.tether.endpoint
}

output "cluster_version" {
  description = "Kubernetes version the cluster is running"
  value       = digitalocean_kubernetes_cluster.tether.version
}

output "node_pool_size" {
  description = "Droplet size slug of the worker nodes"
  value       = digitalocean_kubernetes_cluster.tether.node_pool[0].size
}