package com.ai.web.client.tracing.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.LocalDate;
import java.util.List;

@Data
@Builder(toBuilder = true)
@JsonDeserialize(builder = MemberInfo.MemberInfoBuilder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MemberInfo {

    private String id;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private Long memberId;

    private StatusType statusType;

    private String givenName;

    private String familyName;

    private LocalDate dateOfBirth;

    private Address address;

    private List<? extends Benefit> products;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private long creationMillis;

    @Data
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonDeserialize(builder = Address.AddressBuilder.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Address {

        private String city;

        private String state;

        private String country;

        private String postalCode;

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Benefit {

        private String code;

        private String name;

        protected BenefitType getType() {
            return BenefitType.NONE;
        }

        public interface BenefitBuilder {

            BenefitBuilder code(String code);

            BenefitBuilder name(String name);

            Benefit build();

        }

    }

    @NoArgsConstructor
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonDeserialize(builder = DiscountBenefit.DiscountBenefitBuilder.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class DiscountBenefit extends Benefit {

        @Builder
        public DiscountBenefit(String code, String name) {
            super(code, name);
        }

        @Override
        public BenefitType getType() {
            return BenefitType.DISCOUNT;
        }

        @JsonPOJOBuilder(withPrefix = "")
        public static class DiscountBenefitBuilder implements BenefitBuilder {
        }

    }

    @NoArgsConstructor
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonDeserialize(builder = GiftBenefit.GiftBenefitBuilder.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class GiftBenefit extends Benefit {

        @Builder
        public GiftBenefit(String code, String name) {
            super(code, name);
        }

        @Override
        public BenefitType getType() {
            return BenefitType.GIFT;
        }

        @JsonPOJOBuilder(withPrefix = "")
        public static class GiftBenefitBuilder implements BenefitBuilder {
        }

    }

    public enum BenefitType {
        DISCOUNT, GIFT, NONE
    }

    public enum StatusType {
        ACTIVATED, INACTIVE, RETIRED
    }

}
