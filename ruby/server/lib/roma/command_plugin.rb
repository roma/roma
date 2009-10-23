module Roma
  module CommandPlugin
    @@plugins=[]
    def self.plugins
      @@plugins.dup
    end
    def self.included(mod)
      @@plugins << mod
    end
  end
end
