defmodule DataBuffer.FlusherPool do
  @moduledoc false

  use DynamicSupervisor

  alias DataBuffer.Flusher

  ################################
  # Public API
  ################################

  @spec start_link(any) :: Supervisor.on_start()
  def start_link(buffer) do
    name = flusher_pool_name(buffer)
    DynamicSupervisor.start_link(__MODULE__, :ok, name: name)
  end

  @spec start_flusher(DataBuffer.t(), keyword()) :: DynamicSupervisor.on_start_child()
  def start_flusher(buffer, flush_opts \\ []) do
    name = flusher_pool_name(buffer)
    DynamicSupervisor.start_child(name, {Flusher, [buffer, flush_opts]})
  end

  ################################
  # DynamicSupervisor Callbacks
  ################################

  @impl DynamicSupervisor
  def init(:ok) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  ################################
  # Private API
  ################################

  defp flusher_pool_name(buffer), do: :"#{buffer}.FlusherPool"
end
