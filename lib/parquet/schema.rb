# frozen_string_literal: true

module Parquet
  # Schema definition for Parquet files
  class Schema
    # Define a new schema using the DSL
    # @return [Hash] schema definition hash
    #
    # @example Define a schema with nullable and non-nullable fields
    #   Parquet::Schema.define do
    #     field :id, :int64, nullable: false  # ID cannot be null
    #     field :name, :string  # Default nullable: true
    #
    #     # List with non-nullable items
    #     field :scores, :list, item: :float, item_nullable: false
    #
    #     # Map with nullable values
    #     field :metadata, :map,
    #           key: :string,
    #           value: :string,
    #           value_nullable: true
    #
    #     # Nested struct with non-nullable fields
    #     field :address, :struct, nullable: true do
    #       field :street, :string, nullable: false
    #       field :city, :string, nullable: false
    #       field :zip, :string, nullable: false
    #     end
    #   end
    def self.define(&block)
      builder = SchemaBuilder.new
      builder.instance_eval(&block)

      # Return a structured hash representing the schema
      { type: :struct, fields: builder.fields }
    end

    # Internal builder class that provides the DSL methods
    class SchemaBuilder
      attr_reader :fields

      def initialize
        @fields = []
      end

      # Define a field in the schema
      # @param name [String, Symbol] field name
      # @param type [Symbol] data type (:int32, :int64, :string, :list, :map, :struct, etc)
      # @param nullable [Boolean] whether the field can be null (default: true)
      # @param kwargs [Hash] additional options depending on type
      #
      # Additional keyword args:
      #   - `item:` if type == :list
      #   - `item_nullable:` controls nullability of list items (default: true)
      #   - `key:, value:` if type == :map
      #   - `key_nullable:, value_nullable:` controls nullability of map keys/values (default: true)
      #   - `format:` if you want to store some format string
      #   - `nullable:` default to true if not specified
      def field(name, type, nullable: true, **kwargs, &block)
        field_hash = { name: name.to_s, type: type, nullable: !!nullable }

        # Possibly store a format if provided
        field_hash[:format] = kwargs[:format] if kwargs.key?(:format)

        case type
        when :struct
          # We'll parse subfields from the block
          sub_builder = SchemaBuilder.new
          sub_builder.instance_eval(&block) if block
          field_hash[:fields] = sub_builder.fields
        when :list
          item_type = kwargs[:item]
          raise ArgumentError, "list field `#{name}` requires `item:` type" unless item_type
          # Pass item_nullable if provided, otherwise use true as default
          item_nullable = kwargs[:item_nullable].nil? ? true : !!kwargs[:item_nullable]
          field_hash[:item] = wrap_subtype(item_type, nullable: item_nullable, &block)
        when :map
          # user must specify key:, value:
          key_type = kwargs[:key]
          value_type = kwargs[:value]
          raise ArgumentError, "map field `#{name}` requires `key:` and `value:`" if key_type.nil? || value_type.nil?
          # Pass key_nullable and value_nullable if provided, otherwise use true as default
          key_nullable = kwargs[:key_nullable].nil? ? true : !!kwargs[:key_nullable]
          value_nullable = kwargs[:value_nullable].nil? ? true : !!kwargs[:value_nullable]
          field_hash[:key] = wrap_subtype(key_type, nullable: key_nullable)
          field_hash[:value] = wrap_subtype(value_type, nullable: value_nullable, &block)
        else
          # primitive type: :int32, :int64, :string, etc.
          # do nothing else special
        end

        @fields << field_hash
      end

      def build_map(key_type, value_type, key_nullable: false, value_nullable: true, nullable: true, &block)
        # Wrap the key type (maps typically use non-nullable keys)
        key = wrap_subtype(key_type, nullable: key_nullable)

        # Handle the case where value_type is a complex type (:struct or :list) and a block is provided
        value =
          if (value_type == :struct || value_type == :list) && block
            wrap_subtype(value_type, nullable: value_nullable, &block)
          else
            wrap_subtype(value_type, nullable: value_nullable)
          end

        # Map is represented as a list of key/value pairs in Parquet
        {
          type: :map,
          nullable: nullable,
          item: {
            type: :struct,
            nullable: false,
            name: "key_value",
            fields: [key, value]
          }
        }
      end

      private

      # If user said: field "something", :list, item: :struct do ... end
      # we want to recursively parse that sub-struct from the block.
      # So wrap_subtype might be:
      def wrap_subtype(t, nullable: true, &block)
        if t == :struct
          sub_builder = SchemaBuilder.new
          sub_builder.instance_eval(&block) if block

          # Validate that the struct has at least one field
          if sub_builder.fields.empty?
            raise ArgumentError, "Cannot create a struct with zero fields. Parquet doesn't support empty structs."
          end

          { type: :struct, nullable: nullable, name: "item", fields: sub_builder.fields }
        elsif t == :list && block
          # Handle nested lists by processing the block to define the item type
          sub_builder = SchemaBuilder.new
          sub_builder.instance_eval(&block) if block

          # We expect a single field named "item" that defines the inner list's item type
          if sub_builder.fields.empty? || sub_builder.fields.length > 1 || sub_builder.fields[0][:name] != "item"
            raise ArgumentError, "Nested list must define exactly one field named 'item' for the inner list's item type"
          end

          { type: :list, nullable: nullable, name: "item", item: sub_builder.fields[0] }
        else
          # e.g. :int32 => { type: :int32, nullable: true }
          { type: t, nullable: nullable, name: "item" }
        end
      end
    end
  end
end
