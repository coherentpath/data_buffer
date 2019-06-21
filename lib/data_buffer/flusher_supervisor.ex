defmodule DataBuffer.FlusherSupervisor do
  @moduledoc false

  use DynamicSupervisor

  ################################
  # Public API
  ################################

  @spec start_link(buffer :: DataBuffer.t()) :: Supervisor.on_start()
  def start_link(buffer) do
    DynamicSupervisor.start_link(__MODULE__, [], name: name(buffer))
  end

  def start_flusher(buffer, key, p, opts \\ []) do
    buffer
    |> name()
    |> DynamicSupervisor.start_child({DataBuffer.Flusher, [buffer, key, p, opts]})
  end

  ################################
  # DynamicSupervisor Callbacks
  ################################

  @doc false
  @impl DynamicSupervisor
  def init(_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  ################################
  # Private Functions
  ################################

  defp name(buffer) do
    Module.concat(buffer, FlusherSupervisor)
  end
end
