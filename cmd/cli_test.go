package cmd

import (
	"fmt"
	config2 "github.com/evgeny/consul-replicate/config"
	"io/ioutil"
	"os"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/hashicorp/consul-template/config"
	"github.com/hashicorp/go-gatedio"
)

func TestCLI_ParseFlags(t *testing.T) {
	t.Parallel()

	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	cases := []struct {
		name string
		f    []string
		e    *config2.Config
		err  bool
	}{
		// Deprecations
		// TODO: remove this in 0.8.0
		{
			"auth",
			[]string{"-auth", "abcd:efgh"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					Auth: &config.AuthConfig{
						Username: config.String("abcd"),
						Password: config.String("efgh"),
					},
				},
			},
			false,
		},
		{
			"consul",
			[]string{"-consul", "127.0.0.1:8500"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					Address: config.String("127.0.0.1:8500"),
				},
			},
			false,
		},
		{
			"retry",
			[]string{"-retry", "10s"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					Retry: &config.RetryConfig{
						Backoff:    config.TimeDuration(10 * time.Second),
						MaxBackoff: config.TimeDuration(10 * time.Second),
					},
				},
			},
			false,
		},
		{
			"ssl",
			[]string{"-ssl"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					SSL: &config.SSLConfig{
						Enabled: config.Bool(true),
					},
				},
			},
			false,
		},
		{
			"ssl_verify",
			[]string{"-ssl-verify"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					SSL: &config.SSLConfig{
						Verify: config.Bool(true),
					},
				},
			},
			false,
		},
		{
			"ssl_ca-cert",
			[]string{"-ssl-ca-cert", "foo"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					SSL: &config.SSLConfig{
						CaCert: config.String("foo"),
					},
				},
			},
			false,
		},
		{
			"ssl_cert",
			[]string{"-ssl-cert", "foo"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					SSL: &config.SSLConfig{
						Cert: config.String("foo"),
					},
				},
			},
			false,
		},
		{
			"token",
			[]string{"-token", "abcd1234"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					Token: config.String("abcd1234"),
				},
			},
			false,
		},
		// End Depreations
		// TODO remove in 0.8.0

		{
			"config",
			[]string{"-config", f.Name()},
			&config2.Config{},
			false,
		},
		{
			"config_multi",
			[]string{
				"-config", f.Name(),
				"-config", f.Name(),
			},
			&config2.Config{},
			false,
		},
		{
			"consul_addr",
			[]string{"-consul-addr", "1.2.3.4"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					Address: config.String("1.2.3.4"),
				},
			},
			false,
		},
		{
			"consul_auth_empty",
			[]string{"-consul-auth", ""},
			nil,
			true,
		},
		{
			"consul_auth_username",
			[]string{"-consul-auth", "username"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					Auth: &config.AuthConfig{
						Username: config.String("username"),
					},
				},
			},
			false,
		},
		{
			"consul_auth_username_password",
			[]string{"-consul-auth", "username:password"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					Auth: &config.AuthConfig{
						Username: config.String("username"),
						Password: config.String("password"),
					},
				},
			},
			false,
		},
		{
			"consul-retry",
			[]string{"-consul-retry"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					Retry: &config.RetryConfig{
						Enabled: config.Bool(true),
					},
				},
			},
			false,
		},
		{
			"consul-retry-attempts",
			[]string{"-consul-retry-attempts", "20"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					Retry: &config.RetryConfig{
						Attempts: config.Int(20),
					},
				},
			},
			false,
		},
		{
			"consul-retry-backoff",
			[]string{"-consul-retry-backoff", "30s"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					Retry: &config.RetryConfig{
						Backoff: config.TimeDuration(30 * time.Second),
					},
				},
			},
			false,
		},
		{
			"consul-retry-max-backoff",
			[]string{"-consul-retry-max-backoff", "60s"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					Retry: &config.RetryConfig{
						MaxBackoff: config.TimeDuration(60 * time.Second),
					},
				},
			},
			false,
		},
		{
			"consul-ssl",
			[]string{"-consul-ssl"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					SSL: &config.SSLConfig{
						Enabled: config.Bool(true),
					},
				},
			},
			false,
		},
		{
			"consul-ssl-ca-cert",
			[]string{"-consul-ssl-ca-cert", "ca_cert"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					SSL: &config.SSLConfig{
						CaCert: config.String("ca_cert"),
					},
				},
			},
			false,
		},
		{
			"consul-ssl-ca-path",
			[]string{"-consul-ssl-ca-path", "ca_path"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					SSL: &config.SSLConfig{
						CaPath: config.String("ca_path"),
					},
				},
			},
			false,
		},
		{
			"consul-ssl-cert",
			[]string{"-consul-ssl-cert", "cert"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					SSL: &config.SSLConfig{
						Cert: config.String("cert"),
					},
				},
			},
			false,
		},
		{
			"consul-ssl-key",
			[]string{"-consul-ssl-key", "key"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					SSL: &config.SSLConfig{
						Key: config.String("key"),
					},
				},
			},
			false,
		},
		{
			"consul-ssl-server-name",
			[]string{"-consul-ssl-server-name", "server_name"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					SSL: &config.SSLConfig{
						ServerName: config.String("server_name"),
					},
				},
			},
			false,
		},
		{
			"consul-ssl-verify",
			[]string{"-consul-ssl-verify"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					SSL: &config.SSLConfig{
						Verify: config.Bool(true),
					},
				},
			},
			false,
		},
		{
			"consul-token",
			[]string{"-consul-token", "token"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					Token: config.String("token"),
				},
			},
			false,
		},
		{
			"consul-transport-dial-keep-alive",
			[]string{"-consul-transport-dial-keep-alive", "30s"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					Transport: &config.TransportConfig{
						DialKeepAlive: config.TimeDuration(30 * time.Second),
					},
				},
			},
			false,
		},
		{
			"consul-transport-dial-timeout",
			[]string{"-consul-transport-dial-timeout", "30s"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					Transport: &config.TransportConfig{
						DialTimeout: config.TimeDuration(30 * time.Second),
					},
				},
			},
			false,
		},
		{
			"consul-transport-disable-keep-alives",
			[]string{"-consul-transport-disable-keep-alives"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					Transport: &config.TransportConfig{
						DisableKeepAlives: config.Bool(true),
					},
				},
			},
			false,
		},
		{
			"consul-transport-max-idle-conns-per-host",
			[]string{"-consul-transport-max-idle-conns-per-host", "100"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					Transport: &config.TransportConfig{
						MaxIdleConnsPerHost: config.Int(100),
					},
				},
			},
			false,
		},
		{
			"consul-transport-tls-handshake-timeout",
			[]string{"-consul-transport-tls-handshake-timeout", "30s"},
			&config2.Config{
				Consul: &config.ConsulConfig{
					Transport: &config.TransportConfig{
						TLSHandshakeTimeout: config.TimeDuration(30 * time.Second),
					},
				},
			},
			false,
		},
		{
			"exclude",
			[]string{"-exclude", "foo"},
			&config2.Config{
				Excludes: &config2.ExcludeConfigs{
					&config2.ExcludeConfig{
						Source: config.String("foo"),
					},
				},
			},
			false,
		},
		{
			"exclude_multi",
			[]string{
				"-exclude", "foo",
				"-exclude", "bar",
			},
			&config2.Config{
				Excludes: &config2.ExcludeConfigs{
					&config2.ExcludeConfig{
						Source: config.String("foo"),
					},
					&config2.ExcludeConfig{
						Source: config.String("bar"),
					},
				},
			},
			false,
		},
		{
			"kill-signal",
			[]string{"-kill-signal", "SIGUSR1"},
			&config2.Config{
				KillSignal: config.Signal(syscall.SIGUSR1),
			},
			false,
		},
		{
			"log-level",
			[]string{"-log-level", "DEBUG"},
			&config2.Config{
				LogLevel: config.String("DEBUG"),
			},
			false,
		},
		{
			"max-stale",
			[]string{"-max-stale", "10s"},
			&config2.Config{
				MaxStale: config.TimeDuration(10 * time.Second),
			},
			false,
		},
		{
			"pid-file",
			[]string{"-pid-file", "/var/pid/file"},
			&config2.Config{
				PidFile: config.String("/var/pid/file"),
			},
			false,
		},
		{
			"prefix",
			[]string{"-prefix", "foo/bar@dc1"},
			&config2.Config{
				Prefixes: &config2.PrefixConfigs{
					&config2.PrefixConfig{
						Datacenter:  config.String("dc1"),
						Destination: config.String("foo/bar"),
						Source:      config.String("foo/bar"),
					},
				},
			},
			false,
		},
		{
			"prefix_destination",
			[]string{"-prefix", "foo/bar@dc1:destination"},
			&config2.Config{
				Prefixes: &config2.PrefixConfigs{
					&config2.PrefixConfig{
						Datacenter:  config.String("dc1"),
						Destination: config.String("destination"),
						Source:      config.String("foo/bar"),
					},
				},
			},
			false,
		},
		{
			"prefix_multi",
			[]string{
				"-prefix", "foo/bar@dc",
				"-prefix", "zip/zap@dc",
			},
			&config2.Config{
				Prefixes: &config2.PrefixConfigs{
					&config2.PrefixConfig{
						Datacenter:  config.String("dc"),
						Destination: config.String("foo/bar"),
						Source:      config.String("foo/bar"),
					},
					&config2.PrefixConfig{
						Datacenter:  config.String("dc"),
						Destination: config.String("zip/zap"),
						Source:      config.String("zip/zap"),
					},
				},
			},
			false,
		},
		{
			"reload-signal",
			[]string{"-reload-signal", "SIGUSR1"},
			&config2.Config{
				ReloadSignal: config.Signal(syscall.SIGUSR1),
			},
			false,
		},
		{
			"status-dir",
			[]string{"-status-dir", "a/b/c"},
			&config2.Config{
				StatusDir: config.String("a/b/c"),
			},
			false,
		},
		{
			"syslog",
			[]string{"-syslog"},
			&config2.Config{
				Syslog: &config.SyslogConfig{
					Enabled: config.Bool(true),
				},
			},
			false,
		},
		{
			"syslog-facility",
			[]string{"-syslog-facility", "LOCAL0"},
			&config2.Config{
				Syslog: &config.SyslogConfig{
					Facility: config.String("LOCAL0"),
				},
			},
			false,
		},
		{
			"wait_min",
			[]string{"-wait", "10s"},
			&config2.Config{
				Wait: &config.WaitConfig{
					Min: config.TimeDuration(10 * time.Second),
					Max: config.TimeDuration(40 * time.Second),
				},
			},
			false,
		},
		{
			"wait_min_max",
			[]string{"-wait", "10s:30s"},
			&config2.Config{
				Wait: &config.WaitConfig{
					Min: config.TimeDuration(10 * time.Second),
					Max: config.TimeDuration(30 * time.Second),
				},
			},
			false,
		},
	}

	for i, tc := range cases {
		t.Run(fmt.Sprintf("%d_%s", i, tc.name), func(t *testing.T) {
			out := gatedio.NewByteBuffer()
			cli := NewCLI(nil, out, out)

			a, _, _, _, err := cli.ParseFlags(tc.f)
			if (err != nil) != tc.err {
				t.Fatal(err)
			}

			if tc.e != nil {
				tc.e = config2.DefaultConfig().Merge(tc.e)
			}

			// Nil out dependencies, since they don't compare well
			if a != nil && a.Prefixes != nil {
				for _, p := range *a.Prefixes {
					p.Dependency = nil
				}
			}

			if !reflect.DeepEqual(tc.e, a) {
				t.Errorf("\nexp: %#v\nact: %#v\nout: %q", tc.e, a, out.String())
			}
		})
	}
}
