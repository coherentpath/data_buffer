defmodule DataBuffer.Supervisor do
  @moduledoc false

  use Supervisor

  ################################
  # Public API
  ################################

  @spec start_link(buffer :: DataBuffer.t(), keyword()) :: Supervisor.on_start()
  def start_link(buffer, opts) do
    Supervisor.start_link(__MODULE__, {buffer, opts}, name: name(buffer))
  end

  ################################
  # Supervisor Callbacks
  ################################

  @impl Supervisor
  def init({buffer, opts}) do
    children = [
      {DataBuffer.FlusherSupervisor, buffer},
      {DataBuffer.Worker, [buffer, opts]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  ################################
  # Private Functions
  ################################

  defp name(buffer) do
    Module.concat(buffer, Supervisor)
  end
end
