require 'singleton'

module Roma
  class DNSCache
    include Singleton

    def initialize
      @@addrs = {}
      @@enabled_caching = false
      if Config.const_defined?(:DNS_CACHING)
        @@enabled_caching = Config::DNS_CACHING
      end
    end

    def resolve_name(host)
      return host unless @@enabled_caching

      unless @@addrs.include?(host)
        res = TCPSocket.gethostbyname(host)
        @@addrs[host] = res[3]
      end
      @@addrs[host]
    end

    def disable_dns_cache
      @@enabled_caching = false
      @@addrs.clear
    end

    def enable_dns_cache
      @@enabled_caching = true
    end

    def get_stat
      ret = {}
      ret["dns_caching"] = @@enabled_caching
      ret
    end
  end
end
