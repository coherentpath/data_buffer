defmodule DataBuffer.MixProject do
  use Mix.Project

  @version "0.7.1"

  def project do
    [
      app: :data_buffer,
      version: @version,
      elixir: "~> 1.16",
      elixirc_paths: elixirc_paths(Mix.env()),
      dialyzer: dialyzer(),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      preferred_cli_env: preferred_cli_env(),
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

  defp dialyzer do
    [
      plt_file: {:no_warn, "dialyzer/dialyzer.plt"},
      plt_add_apps: [:ex_unit, :mix]
    ]
  end

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

  # Aliases are shortcuts or tasks specific to the current project.
  defp aliases do
    [
      setup: [
        "local.hex --if-missing --force",
        "local.rebar --if-missing --force",
        "deps.get"
      ],
      ci: [
        "setup",
        "compile --warnings-as-errors",
        "format --check-formatted",
        "credo --strict",
        "test",
        "dialyzer --format github",
        "sobelow --config"
      ]
    ]
  end

  # Specifies the preferred env for mix commands.
  defp preferred_cli_env do
    [
      ci: :test
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:keyword_validator, "~> 2.0"},
      {:telemetry, "~> 1.2.1"},
      {:benchee, "~> 1.0", only: :dev},
      {:ex_doc, "~> 0.22", only: :dev, runtime: false},
      {:credo, "~> 1.7.0", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4.1", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.13.0", only: [:dev, :test], runtime: false}
    ]
  end
end
