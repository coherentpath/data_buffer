defmodule DataBuffer.MixProject do
  use Mix.Project

  @version "0.5.0"

  def project do
    [
      app: :data_buffer,
      version: @version,
      elixir: "~> 1.9",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      name: "DataBuffer",
      docs: docs()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp description do
    """
    DataBuffer provides an efficient way to buffer persistable data.
    """
  end

  defp docs do
    [
      extras: ["README.md"],
      main: "readme",
      source_url: "https://github.com/nsweeting/data_buffer"
    ]
  end

  defp package do
    [
      files: ["lib", "mix.exs", "README*"],
      maintainers: ["Nicholas Sweeting"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/nsweeting/data_buffer"}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:keyword_validator, "~> 1.0"},
      {:telemetry, "~> 0.4"},
      {:benchee, "~> 1.0", only: :dev},
      {:ex_doc, "~> 0.22", only: :dev, runtime: false}
    ]
  end
end
