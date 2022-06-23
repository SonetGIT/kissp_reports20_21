using Intersoft.CISSA.DataAccessLayer.Model.Context;
using Intersoft.CISSA.DataAccessLayer.Model.Documents;
using Intersoft.CISSA.DataAccessLayer.Model.Query;
using Intersoft.CISSA.DataAccessLayer.Model.Query.Builders;
using Intersoft.CISSA.DataAccessLayer.Model.Query.Sql;
using Intersoft.CISSA.DataAccessLayer.Model.Workflow;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Web;
using System.Runtime.Serialization;
using Intersoft.CISSA.DataAccessLayer.Model;

namespace SourcesAPI.Utils
{
    public static class ReportExecutor
    {
        public static class GenerateSummaryReport
        {
            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {
                var ui = context.GetUserInfo();
                var items = new List<ReportItem>
            {
                GetReportItem1(context, year, month, ui.OrganizationId.Value),
                GetReportItem2(context, year, month, ui.OrganizationId.Value),
                GetReportItem3(context, year, month, ui.OrganizationId.Value),
                GetReportItem4(context, year, month, ui.OrganizationId.Value),
                GetReportItem5(context, year, month, ui.OrganizationId.Value),
                GetReportItem6(context, year, month, ui.OrganizationId.Value),
                GetReportItem7(context, year, month, ui.OrganizationId.Value)
            };

                return items;
            }
            //Report Defs
            static Guid reportNo1DefId = new Guid("{1A09ECD6-55E1-4307-862E-6F98F47E252C}");//Уй-булого комок (от 0 до 16 лет)
            static Guid reportNo1ItemDefId = new Guid("{2A8709AB-3522-4019-A29F-5C333893645B}");//Rows Уй-булого комок (от 0 до 16 лет)

            static Guid reportNo2DefId = new Guid("{0C6A34A9-CF41-4750-B21D-9913672A0C76}");//ЕПМС
            static Guid reportNo2ItemDefId = new Guid("{760233C7-3FC3-4417-95AE-F399B8B0208F}");//Rows ЕПМС

            static Guid reportNo3DefId = new Guid("{9118D82A-2AB4-40F2-A3BC-0BB54D34F3CE}");//Социальное пособие  (АСЖ)
            static Guid reportNo3ItemDefId = new Guid("{54167C45-6382-460E-8D72-CDE7D7B43F5C}");//Rows Социальное пособие  (АСЖ)

            static Guid reportNo4DefId = new Guid("{9580E9AE-5949-4B83-90F0-EED511B63477}");//ДЕСП
            static Guid reportNo4ItemDefId = new Guid("{D2FCA75E-34E4-4E85-93B5-6917C4F18BC2}");//Rows ДЕСП


            class _ReportDef
            {
                public Guid ReportDefId { get; set; }
                public Guid ReportItemDefId { get; set; }
            }

            // States
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            static List<ReportItem> GetReportItems(WorkflowContext context, int year, int month, Guid orgId)
            {
                var reportDefs = new List<_ReportDef>
            {
            new _ReportDef
            {
                ReportDefId = reportNo1DefId,
                ReportItemDefId = reportNo1ItemDefId
            },
            /*new _ReportDef
            {
                ReportDefId = reportNo2DefId,
                ReportItemDefId = reportNo2ItemDefId
            },*/
            new _ReportDef
            {
                ReportDefId = reportNo3DefId,
                ReportItemDefId = reportNo3ItemDefId
            }
        };
                var items = new List<ReportItem>();

                foreach (var reportDef in reportDefs)
                {
                    var qb = new QueryBuilder(reportDef.ReportDefId, context.UserId);
                    qb.Where("&OrgId").Eq(orgId)
                        .And("&State").Eq(approvedStateId)
                        .And("Year").Eq(year).And("Month").Eq(month);
                    var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                    var itemSrc = query.JoinSource(query.Source, reportDef.ReportItemDefId, SqlSourceJoinType.Inner, "Rows");
                    if (reportDef.ReportDefId == reportNo1DefId)
                        query.AddCondition(ExpressionOperation.And, reportDef.ReportItemDefId, "PaymentType", ConditionOperation.Equal, new Guid("{330FA388-7596-4D4B-903B-33D4D069707D}"));
                    query.AddAttribute(itemSrc, "PostSection2DocCount", SqlQuerySummaryFunction.Sum);
                    query.AddAttribute(itemSrc, "PostSection2AppCount", SqlQuerySummaryFunction.Sum);
                    query.AddAttribute(itemSrc, "PostSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                    query.AddAttribute(itemSrc, "PostSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                    query.AddAttribute(itemSrc, "PostPercent", SqlQuerySummaryFunction.Sum);
                    query.AddAttribute(itemSrc, "BankSection2DocCount", SqlQuerySummaryFunction.Sum);
                    query.AddAttribute(itemSrc, "BankSection2AppCount", SqlQuerySummaryFunction.Sum);
                    query.AddAttribute(itemSrc, "BankSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                    query.AddAttribute(itemSrc, "BankSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                    query.AddAttribute(itemSrc, "BankPercent", SqlQuerySummaryFunction.Sum);
                    using (var reader = new SqlQueryReader(context.DataContext, query))
                    {
                        if (reader.Read())
                        {
                            items.Add(new ReportItem
                            {
                                RowNo = reportDef.ReportDefId == reportNo1DefId ? 1 : 4,
                                PostFamilyCount = reader.IsDbNull(0) ? 0 : reader.GetInt32(0),
                                PostChildCount = reader.IsDbNull(1) ? 0 : reader.GetInt32(1),
                                PostNeedAmountSec1 = reader.IsDbNull(2) ? 0m : Math.Round(reader.GetDecimal(2) * 0.001m, 2),
                                PostNeedAmountSec2 = reader.IsDbNull(3) ? 0m : Math.Round(reader.GetDecimal(3) * 0.001m, 2),
                                PostComissionSize = reader.IsDbNull(4) ? 0m : Math.Round(reader.GetDecimal(4) * 0.001m, 2),
                                BankFamilyCount = reader.IsDbNull(5) ? 0 : reader.GetInt32(5),
                                BankChildCount = reader.IsDbNull(6) ? 0 : reader.GetInt32(6),
                                BankNeedAmountSec1 = reader.IsDbNull(7) ? 0m : Math.Round(reader.GetDecimal(7) * 0.001m, 2),
                                BankNeedAmountSec2 = reader.IsDbNull(8) ? 0m : Math.Round(reader.GetDecimal(8) * 0.001m, 2),
                                BankComissionSize = reader.IsDbNull(9) ? 0m : Math.Round(reader.GetDecimal(9) * 0.001m, 2)
                            });
                        }
                    }
                }
                GetReportItems2(context, year, month, orgId, items, new object[] { poorBenefitPaymentId, underWardBenefitPaymentId }, true);
                GetReportItems3(context, year, month, orgId, items, new object[] { despCategoryType1Id }, 5);
                return items;
            }

            static ReportItem GetReportItem1(WorkflowContext context, int year, int month, Guid orgId)
            {
                var qb = new QueryBuilder(reportNo1DefId, context.UserId);
                qb.Where("&OrgId").Eq(orgId)
                    .And("&State").Eq(approvedStateId)
                    .And("Year").Eq(year).And("Month").Eq(month);
                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var itemSrc = query.JoinSource(query.Source, reportNo1ItemDefId, SqlSourceJoinType.Inner, "Rows");
                query.AddCondition(ExpressionOperation.And, reportNo1ItemDefId, "PaymentType", ConditionOperation.Equal, new Guid("{330FA388-7596-4D4B-903B-33D4D069707D}"));
                query.AddAttribute(itemSrc, "PostSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostPercent", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankPercent", SqlQuerySummaryFunction.Sum);
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    if (reader.Read())
                    {
                        return new ReportItem
                        {
                            RowNo = 1,
                            RowName = "Уй-булого комок (от 0 до 16 лет)",
                            PostFamilyCount = reader.IsDbNull(0) ? 0 : reader.GetInt32(0),
                            PostChildCount = reader.IsDbNull(1) ? 0 : reader.GetInt32(1),
                            PostNeedAmountSec1 = reader.IsDbNull(2) ? 0m : Math.Round(reader.GetDecimal(2) * 0.001m, 2),
                            PostNeedAmountSec2 = reader.IsDbNull(3) ? 0m : Math.Round(reader.GetDecimal(3) * 0.001m, 2),
                            PostComissionSize = reader.IsDbNull(4) ? 0m : Math.Round(reader.GetDecimal(4) * 0.001m, 2),
                            BankFamilyCount = reader.IsDbNull(5) ? 0 : reader.GetInt32(5),
                            BankChildCount = reader.IsDbNull(6) ? 0 : reader.GetInt32(6),
                            BankNeedAmountSec1 = reader.IsDbNull(7) ? 0m : Math.Round(reader.GetDecimal(7) * 0.001m, 2),
                            BankNeedAmountSec2 = reader.IsDbNull(8) ? 0m : Math.Round(reader.GetDecimal(8) * 0.001m, 2),
                            BankComissionSize = reader.IsDbNull(9) ? 0m : Math.Round(reader.GetDecimal(9) * 0.001m, 2)
                        };
                    }
                }
                throw new ApplicationException("Ошибка при инициализации строки 1");
            }
            static ReportItem GetReportItem2(WorkflowContext context, int year, int month, Guid orgId)
            {
                var qb = new QueryBuilder(reportNo2DefId, context.UserId);
                qb.Where("&OrgId").Eq(orgId)
                    .And("&State").Eq(approvedStateId)
                    .And("Year").Eq(year).And("Month").Eq(month);
                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var itemSrc = query.JoinSource(query.Source, reportNo2ItemDefId, SqlSourceJoinType.Inner, "Rows");
                query.AddCondition(ExpressionOperation.And, reportNo2ItemDefId, "PaymentType", ConditionOperation.In, new object[] { poorBenefitPaymentId, underWardBenefitPaymentId });
                query.AddAttribute(itemSrc, "PostSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostPercent", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankPercent", SqlQuerySummaryFunction.Sum);
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    if (reader.Read())
                    {
                        return new ReportItem
                        {
                            RowNo = 2,
                            RowName = "ЕПМС от 3-х до 16-18 лет",
                            PostFamilyCount = reader.IsDbNull(0) ? 0 : reader.GetInt32(0),
                            PostChildCount = reader.IsDbNull(1) ? 0 : reader.GetInt32(1),
                            PostNeedAmountSec1 = reader.IsDbNull(2) ? 0m : Math.Round(reader.GetDecimal(2) * 0.001m, 2),
                            PostNeedAmountSec2 = reader.IsDbNull(3) ? 0m : Math.Round(reader.GetDecimal(3) * 0.001m, 2),
                            PostComissionSize = reader.IsDbNull(4) ? 0m : Math.Round(reader.GetDecimal(4) * 0.001m, 2),
                            BankFamilyCount = reader.IsDbNull(5) ? 0 : reader.GetInt32(5),
                            BankChildCount = reader.IsDbNull(6) ? 0 : reader.GetInt32(6),
                            BankNeedAmountSec1 = reader.IsDbNull(7) ? 0m : Math.Round(reader.GetDecimal(7) * 0.001m, 2),
                            BankNeedAmountSec2 = reader.IsDbNull(8) ? 0m : Math.Round(reader.GetDecimal(8) * 0.001m, 2),
                            BankComissionSize = reader.IsDbNull(9) ? 0m : Math.Round(reader.GetDecimal(9) * 0.001m, 2)
                        };
                    }
                }
                throw new ApplicationException("Ошибка при инициализации строки 2");
            }
            static ReportItem GetReportItem3(WorkflowContext context, int year, int month, Guid orgId)
            {
                var qb = new QueryBuilder(reportNo2DefId, context.UserId);
                qb.Where("&OrgId").Eq(orgId)
                    .And("&State").Eq(approvedStateId)
                    .And("Year").Eq(year).And("Month").Eq(month);
                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var itemSrc = query.JoinSource(query.Source, reportNo2ItemDefId, SqlSourceJoinType.Inner, "Rows");
                query.AddCondition(ExpressionOperation.And, reportNo2ItemDefId, "PaymentType", ConditionOperation.In, new object[] { twinsBenefitPaymentId, till3BenefitPaymentId, tripletsBenefitPaymentId });
                query.AddAttribute(itemSrc, "PostSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostPercent", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankPercent", SqlQuerySummaryFunction.Sum);
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    if (reader.Read())
                    {
                        return new ReportItem
                        {
                            RowNo = 3,
                            RowName = "ЕПМС до 3-х лет",
                            PostFamilyCount = reader.IsDbNull(0) ? 0 : reader.GetInt32(0),
                            PostChildCount = reader.IsDbNull(1) ? 0 : reader.GetInt32(1),
                            PostNeedAmountSec1 = reader.IsDbNull(2) ? 0m : Math.Round(reader.GetDecimal(2) * 0.001m, 2),
                            PostNeedAmountSec2 = reader.IsDbNull(3) ? 0m : Math.Round(reader.GetDecimal(3) * 0.001m, 2),
                            PostComissionSize = reader.IsDbNull(4) ? 0m : Math.Round(reader.GetDecimal(4) * 0.001m, 2),
                            BankFamilyCount = reader.IsDbNull(5) ? 0 : reader.GetInt32(5),
                            BankChildCount = reader.IsDbNull(6) ? 0 : reader.GetInt32(6),
                            BankNeedAmountSec1 = reader.IsDbNull(7) ? 0m : Math.Round(reader.GetDecimal(7) * 0.001m, 2),
                            BankNeedAmountSec2 = reader.IsDbNull(8) ? 0m : Math.Round(reader.GetDecimal(8) * 0.001m, 2),
                            BankComissionSize = reader.IsDbNull(9) ? 0m : Math.Round(reader.GetDecimal(9) * 0.001m, 2)
                        };
                    }
                }
                throw new ApplicationException("Ошибка при инициализации строки 3");
            }
            static ReportItem GetReportItem4(WorkflowContext context, int year, int month, Guid orgId)
            {
                var qb = new QueryBuilder(reportNo3DefId, context.UserId);
                qb.Where("&OrgId").Eq(orgId)
                    .And("&State").Eq(approvedStateId)
                    .And("Year").Eq(year).And("Month").Eq(month);
                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var itemSrc = query.JoinSource(query.Source, reportNo1ItemDefId, SqlSourceJoinType.Inner, "Rows");
                query.AddAttribute(itemSrc, "PostSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostPercent", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankPercent", SqlQuerySummaryFunction.Sum);
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    if (reader.Read())
                    {
                        return new ReportItem
                        {
                            RowNo = 4,
                            RowName = "Социальное пособие  (АСЖ)",
                            PostFamilyCount = reader.IsDbNull(0) ? 0 : reader.GetInt32(0),
                            PostChildCount = reader.IsDbNull(1) ? 0 : reader.GetInt32(1),
                            PostNeedAmountSec1 = reader.IsDbNull(2) ? 0m : Math.Round(reader.GetDecimal(2) * 0.001m, 2),
                            PostNeedAmountSec2 = reader.IsDbNull(3) ? 0m : Math.Round(reader.GetDecimal(3) * 0.001m, 2),
                            PostComissionSize = reader.IsDbNull(4) ? 0m : Math.Round(reader.GetDecimal(4) * 0.001m, 2),
                            BankFamilyCount = reader.IsDbNull(5) ? 0 : reader.GetInt32(5),
                            BankChildCount = reader.IsDbNull(6) ? 0 : reader.GetInt32(6),
                            BankNeedAmountSec1 = reader.IsDbNull(7) ? 0m : Math.Round(reader.GetDecimal(7) * 0.001m, 2),
                            BankNeedAmountSec2 = reader.IsDbNull(8) ? 0m : Math.Round(reader.GetDecimal(8) * 0.001m, 2),
                            BankComissionSize = reader.IsDbNull(9) ? 0m : Math.Round(reader.GetDecimal(9) * 0.001m, 2)
                        };
                    }
                }
                throw new ApplicationException("Ошибка при инициализации строки 4");
            }
            static ReportItem GetReportItem5(WorkflowContext context, int year, int month, Guid orgId)
            {
                var qb = new QueryBuilder(reportNo4DefId, context.UserId);
                qb.Where("&OrgId").Eq(orgId)
                    .And("&State").Eq(approvedStateId)
                    .And("Year").Eq(year).And("Month").Eq(month);
                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var itemSrc = query.JoinSource(query.Source, reportNo4ItemDefId, SqlSourceJoinType.Inner, "Rows");
                query.AddCondition(ExpressionOperation.And, reportNo4ItemDefId, "Category", ConditionOperation.Equal, despCategoryType1Id);
                query.AddAttribute(itemSrc, "PostSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostPercent", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankPercent", SqlQuerySummaryFunction.Sum);
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    if (reader.Read())
                    {
                        return new ReportItem
                        {
                            RowNo = 5,
                            RowName = "До 18 лет",
                            PostFamilyCount = reader.IsDbNull(0) ? 0 : reader.GetInt32(0),
                            PostChildCount = reader.IsDbNull(1) ? 0 : reader.GetInt32(1),
                            PostNeedAmountSec1 = reader.IsDbNull(2) ? 0m : Math.Round(reader.GetDecimal(2) * 0.001m, 2),
                            PostNeedAmountSec2 = reader.IsDbNull(3) ? 0m : Math.Round(reader.GetDecimal(3) * 0.001m, 2),
                            PostComissionSize = reader.IsDbNull(4) ? 0m : Math.Round(reader.GetDecimal(4) * 0.001m, 2),
                            BankFamilyCount = reader.IsDbNull(5) ? 0 : reader.GetInt32(5),
                            BankChildCount = reader.IsDbNull(6) ? 0 : reader.GetInt32(6),
                            BankNeedAmountSec1 = reader.IsDbNull(7) ? 0m : Math.Round(reader.GetDecimal(7) * 0.001m, 2),
                            BankNeedAmountSec2 = reader.IsDbNull(8) ? 0m : Math.Round(reader.GetDecimal(8) * 0.001m, 2),
                            BankComissionSize = reader.IsDbNull(9) ? 0m : Math.Round(reader.GetDecimal(9) * 0.001m, 2)
                        };
                    }
                }
                throw new ApplicationException("Ошибка при инициализации строки 5");
            }
            static ReportItem GetReportItem6(WorkflowContext context, int year, int month, Guid orgId)
            {
                var qb = new QueryBuilder(reportNo4DefId, context.UserId);
                qb.Where("&OrgId").Eq(orgId)
                    .And("&State").Eq(approvedStateId)
                    .And("Year").Eq(year).And("Month").Eq(month);
                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var itemSrc = query.JoinSource(query.Source, reportNo4ItemDefId, SqlSourceJoinType.Inner, "Rows");
                query.AddCondition(ExpressionOperation.And, reportNo4ItemDefId, "Category", ConditionOperation.In, new object[] { despCategoryType3Id, despCategoryType4Id });
                query.AddAttribute(itemSrc, "PostSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostPercent", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankPercent", SqlQuerySummaryFunction.Sum);
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    if (reader.Read())
                    {
                        return new ReportItem
                        {
                            RowNo = 6,
                            RowName = "ДЕСП ЛОВЗ",
                            PostFamilyCount = reader.IsDbNull(0) ? 0 : reader.GetInt32(0),
                            PostChildCount = reader.IsDbNull(1) ? 0 : reader.GetInt32(1),
                            PostNeedAmountSec1 = reader.IsDbNull(2) ? 0m : Math.Round(reader.GetDecimal(2) * 0.001m, 2),
                            PostNeedAmountSec2 = reader.IsDbNull(3) ? 0m : Math.Round(reader.GetDecimal(3) * 0.001m, 2),
                            PostComissionSize = reader.IsDbNull(4) ? 0m : Math.Round(reader.GetDecimal(4) * 0.001m, 2),
                            BankFamilyCount = reader.IsDbNull(5) ? 0 : reader.GetInt32(5),
                            BankChildCount = reader.IsDbNull(6) ? 0 : reader.GetInt32(6),
                            BankNeedAmountSec1 = reader.IsDbNull(7) ? 0m : Math.Round(reader.GetDecimal(7) * 0.001m, 2),
                            BankNeedAmountSec2 = reader.IsDbNull(8) ? 0m : Math.Round(reader.GetDecimal(8) * 0.001m, 2),
                            BankComissionSize = reader.IsDbNull(9) ? 0m : Math.Round(reader.GetDecimal(9) * 0.001m, 2)
                        };
                    }
                }
                throw new ApplicationException("Ошибка при инициализации строки 6");
            }
            static ReportItem GetReportItem7(WorkflowContext context, int year, int month, Guid orgId)
            {
                var qb = new QueryBuilder(reportNo4DefId, context.UserId);
                qb.Where("&OrgId").Eq(orgId)
                    .And("&State").Eq(approvedStateId)
                    .And("Year").Eq(year).And("Month").Eq(month);
                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var itemSrc = query.JoinSource(query.Source, reportNo4ItemDefId, SqlSourceJoinType.Inner, "Rows");
                query.AddCondition(ExpressionOperation.And, reportNo4ItemDefId, "Category", ConditionOperation.Equal, despCategoryType2Id);
                query.AddAttribute(itemSrc, "PostSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostPercent", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankPercent", SqlQuerySummaryFunction.Sum);
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    if (reader.Read())
                    {
                        return new ReportItem
                        {
                            RowNo = 7,
                            RowName = "ДЕСП родители",
                            PostFamilyCount = reader.IsDbNull(0) ? 0 : reader.GetInt32(0),
                            PostChildCount = reader.IsDbNull(1) ? 0 : reader.GetInt32(1),
                            PostNeedAmountSec1 = reader.IsDbNull(2) ? 0m : Math.Round(reader.GetDecimal(2) * 0.001m, 2),
                            PostNeedAmountSec2 = reader.IsDbNull(3) ? 0m : Math.Round(reader.GetDecimal(3) * 0.001m, 2),
                            PostComissionSize = reader.IsDbNull(4) ? 0m : Math.Round(reader.GetDecimal(4) * 0.001m, 2),
                            BankFamilyCount = reader.IsDbNull(5) ? 0 : reader.GetInt32(5),
                            BankChildCount = reader.IsDbNull(6) ? 0 : reader.GetInt32(6),
                            BankNeedAmountSec1 = reader.IsDbNull(7) ? 0m : Math.Round(reader.GetDecimal(7) * 0.001m, 2),
                            BankNeedAmountSec2 = reader.IsDbNull(8) ? 0m : Math.Round(reader.GetDecimal(8) * 0.001m, 2),
                            BankComissionSize = reader.IsDbNull(9) ? 0m : Math.Round(reader.GetDecimal(9) * 0.001m, 2)
                        };
                    }
                }
                throw new ApplicationException("Ошибка при инициализации строки 7");
            }
            // Виды выплат
            private static readonly Guid poorBenefitPaymentId = new Guid("{D24151CF-C8B0-4851-B0EC-6D6EB382DC61}");   // * ЕПМС семьям по малообеспеченности
            private static readonly Guid twinsBenefitPaymentId = new Guid("{7F1B9709-8F99-473F-9AE0-2DDCD74BDE6E}");   // * Пособие матерям родившим двойню до достижения 3-лет
            private static readonly Guid till3BenefitPaymentId = new Guid("{9BC8A898-31F8-4F55-8C14-28F641142370}");   // * ЕПМС на ребенка до 3-х лет
            private static readonly Guid tripletsBenefitPaymentId = new Guid("{64ACC17D-78B8-492E-AC81-7B1E4750F53A}");   // * ЕПМС матерям родившим тройню и более до достижения 16-лет
            private static readonly Guid underWardBenefitPaymentId = new Guid("{BCE5B287-7495-4AD1-96A8-F52040A4CABF}");   // * ЕПМС малоимущим (опекаемым, сиротам) 
            private static readonly Guid onBirthBenefitPaymentId = new Guid("{43F0ED4A-EFF2-425D-8564-683551BA8F82}");   // * Единовременное пособие при рождении ребенка
            private static readonly Guid onBirthBenefitPayment2Id = new Guid("{1D0CD630-9BB0-4716-AEB7-673B54B42CE5}");   // * Суйунчу
            static List<ReportItem> GetReportItems2(WorkflowContext context, int year, int month, Guid orgId, List<ReportItem> items, object[] rowTypes, bool isFirstCalled)
            {
                var qb = new QueryBuilder(reportNo2DefId, context.UserId);
                qb.Where("&OrgId").Eq(orgId)
                    .And("&State").Eq(approvedStateId)
                    .And("Year").Eq(year).And("Month").Eq(month);
                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var itemSrc = query.JoinSource(query.Source, reportNo2ItemDefId, SqlSourceJoinType.Inner, "Rows");
                query.AddCondition(ExpressionOperation.And, reportNo2ItemDefId, "PaymentType", ConditionOperation.In, rowTypes);
                query.AddAttribute(itemSrc, "PostSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostPercent", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankPercent", SqlQuerySummaryFunction.Sum);
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    if (reader.Read())
                    {
                        items.Add(new ReportItem
                        {
                            RowNo = isFirstCalled ? 2 : 3,
                            PostFamilyCount = reader.IsDbNull(0) ? 0 : reader.GetInt32(0),
                            PostChildCount = reader.IsDbNull(1) ? 0 : reader.GetInt32(1),
                            PostNeedAmountSec1 = reader.IsDbNull(2) ? 0m : Math.Round(reader.GetDecimal(2) * 0.001m, 2),
                            PostNeedAmountSec2 = reader.IsDbNull(3) ? 0m : Math.Round(reader.GetDecimal(3) * 0.001m, 2),
                            PostComissionSize = reader.IsDbNull(4) ? 0m : Math.Round(reader.GetDecimal(4) * 0.001m, 2),
                            BankFamilyCount = reader.IsDbNull(5) ? 0 : reader.GetInt32(5),
                            BankChildCount = reader.IsDbNull(6) ? 0 : reader.GetInt32(6),
                            BankNeedAmountSec1 = reader.IsDbNull(7) ? 0m : Math.Round(reader.GetDecimal(7) * 0.001m, 2),
                            BankNeedAmountSec2 = reader.IsDbNull(8) ? 0m : Math.Round(reader.GetDecimal(8) * 0.001m, 2),
                            BankComissionSize = reader.IsDbNull(9) ? 0m : Math.Round(reader.GetDecimal(9) * 0.001m, 2)
                        });
                    }
                }

                return isFirstCalled ? GetReportItems2(context, year, month, orgId, items, new object[] { twinsBenefitPaymentId, till3BenefitPaymentId, tripletsBenefitPaymentId }, false) : items;
            }


            private static readonly Guid despCategoryType1Id = new Guid("{608F740E-C44B-4305-88E8-F53D497214C0}");//Дети погибшего в событиях 2010 г. до 18 лет
            private static readonly Guid despCategoryType2Id = new Guid("{238BD896-A3B7-494D-B549-4AAC292EE8F5}");//Родители погибшего ед. ребенка в событиях 2010 г. по достижении пенсионного возраста
            private static readonly Guid despCategoryType3Id = new Guid("{ECDF4DE6-299E-4C6A-843F-49362C32EB96}");//Получившие вред здоровью в событиях 2010 г., имеющие заключение СМЭ
            private static readonly Guid despCategoryType4Id = new Guid("{7D08F570-239D-46BF-BF4F-6E3A510ACCD2}");//Признанные ЛОВЗ в следствие событий 2010 г.
            static List<ReportItem> GetReportItems3(WorkflowContext context, int year, int month, Guid orgId, List<ReportItem> items, object[] rowTypes, int rowNo)
            {
                var qb = new QueryBuilder(reportNo4DefId, context.UserId);
                qb.Where("&OrgId").Eq(orgId)
                    .And("&State").Eq(approvedStateId)
                    .And("Year").Eq(year).And("Month").Eq(month);
                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var itemSrc = query.JoinSource(query.Source, reportNo4ItemDefId, SqlSourceJoinType.Inner, "Rows");
                query.AddCondition(ExpressionOperation.And, reportNo4ItemDefId, "Category", ConditionOperation.In, rowTypes);
                query.AddAttribute(itemSrc, "PostSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "PostPercent", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2DocCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2AppCount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection1NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute(itemSrc, "BankPercent", SqlQuerySummaryFunction.Sum);
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    if (reader.Read())
                    {
                        items.Add(new ReportItem
                        {
                            RowNo = rowNo,
                            PostFamilyCount = reader.IsDbNull(0) ? 0 : reader.GetInt32(0),
                            PostChildCount = reader.IsDbNull(1) ? 0 : reader.GetInt32(1),
                            PostNeedAmountSec1 = reader.IsDbNull(2) ? 0m : Math.Round(reader.GetDecimal(2) * 0.001m, 2),
                            PostNeedAmountSec2 = reader.IsDbNull(3) ? 0m : Math.Round(reader.GetDecimal(3) * 0.001m, 2),
                            PostComissionSize = reader.IsDbNull(4) ? 0m : Math.Round(reader.GetDecimal(4) * 0.001m, 2),
                            BankFamilyCount = reader.IsDbNull(5) ? 0 : reader.GetInt32(5),
                            BankChildCount = reader.IsDbNull(6) ? 0 : reader.GetInt32(6),
                            BankNeedAmountSec1 = reader.IsDbNull(7) ? 0m : Math.Round(reader.GetDecimal(7) * 0.001m, 2),
                            BankNeedAmountSec2 = reader.IsDbNull(8) ? 0m : Math.Round(reader.GetDecimal(8) * 0.001m, 2),
                            BankComissionSize = reader.IsDbNull(9) ? 0m : Math.Round(reader.GetDecimal(9) * 0.001m, 2)
                        });
                    }
                }

                return rowNo == 5 ? GetReportItems3(context, year, month, orgId, items, new object[] { despCategoryType3Id, despCategoryType4Id }, 6) : rowNo == 6 ? GetReportItems3(context, year, month, orgId, items, new object[] { despCategoryType2Id }, 7) : items;
            }

            public class ReportItem
            {
                public int RowNo { get; set; }
                public string RowName { get; set; }
                #region потребная сумма получателей через почту
                public int PostFamilyCount { get; set; }
                public int PostChildCount { get; set; }
                public decimal PostNeedAmount
                {
                    get
                    {
                        return PostNeedAmountSec1 + PostNeedAmountSec2;
                    }
                }
                public decimal PostNeedAmountSec1 { get; set; }
                public decimal PostNeedAmountSec2 { get; set; }
                public decimal PostComissionSize { get; set; }
                #endregion

                #region потребная сумма получателей через банк
                public int BankFamilyCount { get; set; }
                public int BankChildCount { get; set; }
                public decimal BankNeedAmount
                {
                    get
                    {
                        return BankNeedAmountSec1 + BankNeedAmountSec2;
                    }
                }
                public decimal BankNeedAmountSec1 { get; set; }
                public decimal BankNeedAmountSec2 { get; set; }
                public decimal BankComissionSize { get; set; }
                #endregion

                #region Всего получ-й
                public int TotalFamilyCount
                {
                    get
                    {
                        return PostFamilyCount + BankFamilyCount;
                    }
                }
                public int TotalChildCount
                {
                    get
                    {
                        return PostChildCount + BankChildCount;
                    }
                }
                #endregion

                #region Всего потребная сумма
                public decimal TotalNeedAmount
                {
                    get
                    {
                        return PostNeedAmount + BankNeedAmount;
                    }
                }
                public decimal TotalPostComissionSize
                {
                    get
                    {
                        return PostComissionSize;
                    }
                }
                public decimal TotalBankComissionSize
                {
                    get
                    {
                        return BankComissionSize;
                    }
                }
                public decimal TotalNeedAmountComissionSize
                {
                    get
                    {
                        return TotalNeedAmount + TotalBankComissionSize + TotalPostComissionSize;
                    }
                }
                #endregion

                #region средний размер на 1 человека
                public decimal AvgAmount
                {
                    get
                    {
                        var total = (BankNeedAmountSec2 + PostNeedAmountSec2);
                        if (total > 0 && TotalChildCount > 0)
                            return Math.Round((total / TotalChildCount) * 1000, 2);
                        else
                            return 0;
                    }
                }
                #endregion
            }
        }

        public static class PoorBenefitsReport2
        {
            // Виды выплат                             
            private static readonly Guid uyBulogoKomokPaymentId = new Guid("{3193A90D-380B-4428-B7B1-04C548AA902E}");   // * Уй-булого комок

            // Document Defs
            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{1A09ECD6-55E1-4307-862E-6F98F47E252C}");
            private static readonly Guid reportItemDefId = new Guid("{2A8709AB-3522-4019-A29F-5C333893645B}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");
            private static readonly Guid famMember = new Guid("{85B03F9E-47D7-4829-8041-0CDCB8486572}");
            private static readonly Guid famMemberTypeId1 = new Guid("{9BF3B519-C0B8-4344-BF29-47B062C07454}");
            private static readonly Guid famMemberTypeId2 = new Guid("{45D55628-5E72-42B8-8B8D-667346E79046}");
            private static readonly Guid famMemberTypeId3 = new Guid("{8CCAE21E-128A-4728-9479-9C094271C614}");
            private static readonly Guid empStatusTo3Id = new Guid("{D8FF3DAF-A701-414A-B965-4BF93BB658B9}"); //Ребенок до 3-х лет
            private static readonly Guid empStatusTo16Id = new Guid("{8024982A-4AFB-4074-9D63-3EEFE22420E0}");  //Ребенок от 3-х до 16 лет  

            // States                               
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid canceledStateId = new Guid("{C65E63CC-54E9-424E-AFDC-5A8DB1CB04A3}"); // Аннулирован
            private static readonly Guid onApprovingStateId = new Guid("{5CD9E88D-671E-4A44-AD92-9F74DA3B47F7}"); //На утверждении
            private static readonly Guid onRegisteringStateId = new Guid("{A9CD37C4-A718-4DE1-9E95-EC8EC280C8D4}"); // На регистрации
            private static readonly Guid refusedStateId = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}"); // Отказан

            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)//*
            {
                try
                {
                    var ui = context.GetUserInfo();
                    return Build(year, month, context.UserId, (Guid)ui.OrganizationId, context);
                }
                catch (Exception ex)
                {
                    throw new ApplicationException("TargetSite: " + ex.TargetSite.Name + "; Message: " + ex.Message);//*
                }
            }
            private static Doc GetReportDoc(WorkflowContext context, List<object[]> reports)
            {
                Doc report;
                var docRepo = context.Documents;
                var approvedReportId = reports.FirstOrDefault(r => (Guid)r[1] == approvedStateId);
                if (approvedReportId != null)
                {
                    context["ApprovedReport"] = docRepo.LoadById((Guid)approvedReportId[0]);
                    return null;
                }
                reports = reports.Where(r => (Guid)r[1] == onRegisteringStateId).ToList();
                if (reports.Count > 0)
                {
                    var onRegisteringReportId = reports.FirstOrDefault(x => (DateTime)x[2] == reports.Max(r => (DateTime)r[2]));
                    report = docRepo.LoadById((Guid)onRegisteringReportId[0]);
                    reports.RemoveAll(x => (Guid)x[0] == (Guid)onRegisteringReportId[0]);
                    report["DocCount"] = null;
                    report["AppCount"] = null;
                    report["NeedAmount"] = null;
                    report["Percent"] = null;
                    report["NeedAmountPercent"] = null;
                    docRepo.ClearAttrDocList(report.Id, report.Get<DocListAttribute>("Rows").AttrDef.Id);
                    foreach (var reportId in reports.Select(x => (Guid)x[0]))
                    {
                        docRepo.SetDocState(reportId, refusedStateId);
                    }
                }
                else
                    report = docRepo.New(reportDefId);
                return report;
            }

            static List<ReportItem> InitReport(WorkflowContext context, Doc report, ReportItem _item1, ReportItem _item2, ReportItem _item3, ReportItem _item4)
            {
                var reportItems = new List<Doc>();
                var docRepo = context.Documents;
                foreach (var rItemId in docRepo.DocAttrList(out int c, report, "Rows", 0, 0))
                {
                    reportItems.Add(docRepo.LoadById(rItemId));
                }
                CalcFields(reportItems.FirstOrDefault(x => (Guid)x["PaymentType"] == row1TypeId),
                    reportItems.FirstOrDefault(x => (Guid)x["PaymentType"] == row2TypeId),
                    reportItems.FirstOrDefault(x => (Guid)x["PaymentType"] == row3TypeId),
                    reportItems.FirstOrDefault(x => (Guid)x["PaymentType"] == row4TypeId),
                    report, _item1, _item2, _item3, _item4);
                return new List<ReportItem> { _item1, _item2, _item3, _item4 };
            }
            static Guid row1TypeId = new Guid("{330FA388-7596-4D4B-903B-33D4D069707D}"); //Уй-булого комок от 0 до 16 лет   
            static Guid row2TypeId = new Guid("{3F7B2602-49D9-4611-A64B-D1E1B4873D13}"); //Уй-булого комок  (опекаемым, сиротам)
            static Guid row3TypeId = new Guid("{7FD8CE36-0E0E-4C0B-B21D-6EB0521AB75B}"); //Пособие матерям родившим двойню до 16 лет
            static Guid row4TypeId = new Guid("{8BC11C2E-AD05-4A96-A434-9C4A57CB03F0}"); //Пособие матерям родившим тройню и более до достижения 16-лет

            public static List<ReportItem> Build(int year, int month, Guid userId, Guid orgId, WorkflowContext context)//*
            {
                if (year < 2011 || year > 3000)
                    throw new ApplicationException("Ошибка в значении года!");
                if (month < 1 || month > 12)
                    throw new ApplicationException("Ошибка в значении месяца!");
                var docRepo = context.Documents;
                var qb = new QueryBuilder(reportDefId);
                qb.Where("&OrgId").Eq(orgId).And("Month").Eq(month).And("Year").Eq(year).And("&State").Neq(refusedStateId);

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("&Id");
                query.AddAttribute("&State");
                query.AddAttribute("&Created");
                var reports = new List<object[]>();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        reports.Add(new object[] { reader.GetGuid(0), reader.GetGuid(1), reader.GetDateTime(2) });
                    }
                }
                decimal postPercent = GetPostPercent(year, month, userId, context);
                List<Doc> items = new List<Doc>();

                //Инициализация строк отчета

                //******

                var item1 = docRepo.New(reportItemDefId);
                item1["PaymentType"] = row1TypeId;
                var _item1 = new ReportItem { RowName = context.Enums.GetValue(row1TypeId).Value };//*

                var item2 = docRepo.New(reportItemDefId);
                item2["PaymentType"] = row2TypeId;
                var _item2 = new ReportItem { RowName = context.Enums.GetValue(row2TypeId).Value };//*

                var item3 = docRepo.New(reportItemDefId);
                item3["PaymentType"] = row3TypeId;
                var _item3 = new ReportItem { RowName = context.Enums.GetValue(row3TypeId).Value };//*

                var item4 = docRepo.New(reportItemDefId);
                item4["PaymentType"] = row4TypeId;
                var _item4 = new ReportItem { RowName = context.Enums.GetValue(row4TypeId).Value };//*

                Doc report = GetReportDoc(context, reports);
                if (report == null)
                {
                    return InitReport(context, context.Get<Doc>("ApprovedReport"), _item1, _item2, _item3, _item4);//context.Get<Doc>("ApprovedReport");
                }


                CalcSection1(item1, item2, item3, item4, context, year, month, orgId, userId, postPercent);

                CalcSection2(item1, item2, item3, item4, context, year, month, orgId, userId, postPercent);

                //****
                CalcFields(item1, item2, item3, item4, report, _item1, _item2, _item3, _item4);//*

                items.Reverse();

                foreach (Doc item in new[] { item4, item3, item2, item1 })
                {
                    docRepo.Save(item);
                    docRepo.AddDocToList(item.Id, report, "Rows");
                }
                report["Year"] = year;
                report["Month"] = month;
                report["Organization"] = orgId;

                docRepo.Save(report);
                docRepo.SetDocState(report, onRegisteringStateId);
                //****
                return new List<ReportItem> { _item1, _item2, _item3, _item4 };
            }

            private static void CalcSection1(Doc item1, Doc item2, Doc item3, Doc item4, WorkflowContext context, int year, int month, Guid orgId, Guid userId, decimal postPercent)
            {
                CalcAmountSum(userId, year, month, orgId, item1, item2, item3, item4, postPercent, "Post", context, section1TypeId);
            }

            private static void CalcSection2(Doc item1, Doc item2, Doc item3, Doc item4, WorkflowContext context, int year, int month, Guid orgId, Guid userId, decimal postPercent)
            {
                CalcAppCount(userId, postOrderDefId, year, month, orgId, item1, item2, item3, item4, "Post", postPercent, context, section2TypeId);
                CalcOrdersCount(userId, postOrderDefId, year, month, orgId, item1, "Post", context);
                CalcAmountSum(userId, year, month, orgId, item1, item2, item3, item4, postPercent, "Post", context, section2TypeId);
            }

            private static void CalcFields(Doc item1, Doc item2, Doc item3, Doc item4, Doc report, ReportItem _item1, ReportItem _item2, ReportItem _item3, ReportItem _item4)
            {
                var items = new[] { item4, item3, item2, item1 };
                //
                var _items = new[] { _item4, _item3, _item2, _item1 };

                for (int i = 0; items.Length > i; i++)
                {
                    var item = items[i];
                    var _item = _items[i];
                    FieldsInit(item);
                    item["PostNeedAmount"] = (decimal)item["PostSection1NeedAmount"] + (decimal)item["PostSection2NeedAmount"];
                    item["PostPercent"] = (decimal)item["PostSection1Percent"] + (decimal)item["PostSection2Percent"];
                    item["PostNeedAmountPercent"] = (decimal)item["PostNeedAmount"] + (decimal)item["PostPercent"];

                    item["BankNeedAmount"] = (decimal)item["BankSection1NeedAmount"] + (decimal)item["BankSection2NeedAmount"];
                    item["BankPercent"] = (decimal)item["BankSection1Percent"] + (decimal)item["BankSection2Percent"];
                    item["BankNeedAmountPercent"] = (decimal)item["BankNeedAmount"] + (decimal)item["BankPercent"];

                    item["DocCount"] = (int)item["PostSection2DocCount"] + (int)item["BankSection2DocCount"];
                    item["AppCount"] = (int)item["PostSection2AppCount"] + (int)item["BankSection2AppCount"];
                    item["NeedAmount"] = (decimal)item["PostNeedAmount"] + (decimal)item["BankNeedAmount"];
                    item["Percent"] = (decimal)item["PostPercent"] + (decimal)item["BankPercent"];
                    item["NeedAmountPercent"] = (decimal)item["PostNeedAmountPercent"] + (decimal)item["BankNeedAmountPercent"];

                    FieldsInit(item, _item);
                }
                report["DocCount"] = ((int?)report["DocCount"] ?? 0) + (int)item1["DocCount"];
                report["AppCount"] = ((int?)report["AppCount"] ?? 0) + (int)item1["AppCount"];
                report["NeedAmount"] = ((decimal?)report["NeedAmount"] ?? 0) + (decimal)item1["NeedAmount"];
                report["Percent"] = ((decimal?)report["Percent"] ?? 0) + (decimal)item1["Percent"];
                report["NeedAmountPercent"] = ((decimal?)report["NeedAmountPercent"] ?? 0) + (decimal)item1["NeedAmountPercent"];
            }

            private static void FieldsInit(Doc item, ReportItem _item = null)//*
            {
                item["DocCount"] = (int?)item["DocCount"] ?? 0;
                item["AppCount"] = (int?)item["AppCount"] ?? 0;
                item["NeedAmount"] = (decimal?)item["NeedAmount"] ?? 0;
                item["Percent"] = (decimal?)item["Percent"] ?? 0;
                item["NeedAmountPercent"] = (decimal?)item["NeedAmountPercent"] ?? 0;
                item["PostSection1MonthCount"] = (int?)item["PostSection1MonthCount"] ?? 0;
                item["PostSection1AppCount"] = (int?)item["PostSection1AppCount"] ?? 0;
                item["PostSection1NeedAmount"] = (decimal?)item["PostSection1NeedAmount"] ?? 0;
                item["PostSection1Percent"] = (decimal?)item["PostSection1Percent"] ?? 0;
                item["PostSection2DocCount"] = (int?)item["PostSection2DocCount"] ?? 0;
                item["PostSection2AppCount"] = (int?)item["PostSection2AppCount"] ?? 0;
                item["PostSection2NeedAmount"] = (decimal?)item["PostSection2NeedAmount"] ?? 0;
                item["PostSection2Percent"] = (decimal?)item["PostSection2Percent"] ?? 0;
                item["PostAppCount"] = (int?)item["PostAppCount"] ?? 0;
                item["PostNeedAmount"] = (decimal?)item["PostNeedAmount"] ?? 0;
                item["PostPercent"] = (decimal?)item["PostPercent"] ?? 0;
                item["PostNeedAmountPercent"] = (decimal?)item["PostNeedAmountPercent"] ?? 0;

                item["BankSection1MonthCount"] = (int?)item["BankSection1MonthCount"] ?? 0;
                item["BankSection1AppCount"] = (int?)item["BankSection1AppCount"] ?? 0;
                item["BankSection1NeedAmount"] = (decimal?)item["BankSection1NeedAmount"] ?? 0;
                item["BankSection1Percent"] = (decimal?)item["BankSection1Percent"] ?? 0;
                item["BankSection2DocCount"] = (int?)item["BankSection2DocCount"] ?? 0;
                item["BankSection2AppCount"] = (int?)item["BankSection2AppCount"] ?? 0;
                item["BankSection2NeedAmount"] = (decimal?)item["BankSection2NeedAmount"] ?? 0;
                item["BankSection2Percent"] = (decimal?)item["BankSection2Percent"] ?? 0;
                item["BankAppCount"] = (int?)item["BankAppCount"] ?? 0;
                item["BankNeedAmount"] = (decimal?)item["BankNeedAmount"] ?? 0;
                item["BankPercent"] = (decimal?)item["BankPercent"] ?? 0;
                item["BankNeedAmountPercent"] = (decimal?)item["BankNeedAmountPercent"] ?? 0;

                if (_item != null)
                {
                    _item.DocCount = (int?)item["DocCount"] ?? 0;
                    _item.AppCount = (int?)item["AppCount"] ?? 0;
                    _item.NeedAmount = (decimal?)item["NeedAmount"] ?? 0;
                    _item.Percent = (decimal?)item["Percent"] ?? 0;
                    _item.NeedAmountPercent = (decimal?)item["NeedAmountPercent"] ?? 0;
                    _item.PostSection1MonthCount = (int?)item["PostSection1MonthCount"] ?? 0;
                    _item.PostSection1AppCount = (int?)item["PostSection1AppCount"] ?? 0;
                    _item.PostSection1NeedAmount = (decimal?)item["PostSection1NeedAmount"] ?? 0;
                    _item.PostSection1Percent = (decimal?)item["PostSection1Percent"] ?? 0;
                    _item.PostSection2DocCount = (int?)item["PostSection2DocCount"] ?? 0;
                    _item.PostSection2AppCount = (int?)item["PostSection2AppCount"] ?? 0;
                    _item.PostSection2NeedAmount = (decimal?)item["PostSection2NeedAmount"] ?? 0;
                    _item.PostSection2Percent = (decimal?)item["PostSection2Percent"] ?? 0;
                    _item.PostAppCount = (int?)item["PostAppCount"] ?? 0;
                    _item.PostNeedAmount = (decimal?)item["PostNeedAmount"] ?? 0;
                    _item.PostPercent = (decimal?)item["PostPercent"] ?? 0;
                    _item.PostNeedAmountPercent = (decimal?)item["PostNeedAmountPercent"] ?? 0;

                    _item.BankSection1MonthCount = (int?)item["BankSection1MonthCount"] ?? 0;
                    _item.BankSection1AppCount = (int?)item["BankSection1AppCount"] ?? 0;
                    _item.BankSection1NeedAmount = (decimal?)item["BankSection1NeedAmount"] ?? 0;
                    _item.BankSection1Percent = (decimal?)item["BankSection1Percent"] ?? 0;
                    _item.BankSection2DocCount = (int?)item["BankSection2DocCount"] ?? 0;
                    _item.BankSection2AppCount = (int?)item["BankSection2AppCount"] ?? 0;
                    _item.BankSection2NeedAmount = (decimal?)item["BankSection2NeedAmount"] ?? 0;
                    _item.BankSection2Percent = (decimal?)item["BankSection2Percent"] ?? 0;
                    _item.BankAppCount = (int?)item["BankAppCount"] ?? 0;
                    _item.BankNeedAmount = (decimal?)item["BankNeedAmount"] ?? 0;
                    _item.BankPercent = (decimal?)item["BankPercent"] ?? 0;
                    _item.BankNeedAmountPercent = (decimal?)item["BankNeedAmountPercent"] ?? 0;
                }
            }

            public class ReportItem
            {
                public string RowName { get; set; }
                public int DocCount { get; set; }
                public int AppCount { get; set; }
                public decimal NeedAmount { get; set; }
                public decimal Percent { get; set; }
                public decimal NeedAmountPercent { get; set; }

                public int PostSection1MonthCount { get; set; }
                public int PostSection1AppCount { get; set; }
                public decimal PostSection1NeedAmount { get; set; }
                public decimal PostSection1Percent { get; set; }
                public int PostSection2DocCount { get; set; }
                public int PostSection2AppCount { get; set; }
                public decimal PostSection2NeedAmount { get; set; }
                public decimal PostSection2Percent { get; set; }
                public int PostAppCount { get; set; }
                public decimal PostNeedAmount { get; set; }
                public decimal PostPercent { get; set; }
                public decimal PostNeedAmountPercent { get; set; }

                public int BankSection1MonthCount { get; set; }
                public int BankSection1AppCount { get; set; }
                public decimal BankSection1NeedAmount { get; set; }
                public decimal BankSection1Percent { get; set; }
                public int BankSection2DocCount { get; set; }
                public int BankSection2AppCount { get; set; }
                public decimal BankSection2NeedAmount { get; set; }
                public decimal BankSection2Percent { get; set; }
                public int BankAppCount { get; set; }
                public decimal BankNeedAmount { get; set; }
                public decimal BankPercent { get; set; }
                public decimal BankNeedAmountPercent { get; set; }
            }

            private static void CalcAppCount(Guid userId, Guid orderDefId, int year, int month, Guid orgId, Doc item1, Doc item2, Doc item3, Doc item4, string orderTypeName, decimal postPercent, WorkflowContext context, Guid sectionTypeId)
            {
                SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                var orderSrc = query.JoinSource(query.Source, orderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                var assignSrc = query.JoinSource(appSrc, assignmentDefId, SqlSourceJoinType.Inner, "Assignments");
                //        var famSrc = query.JoinSource(appSrc, famMember, SqlSourceJoinType.Inner, "FamilyMembers");
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                query.AddCondition(ExpressionOperation.And, orderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                query.AddCondition(ExpressionOperation.And, orderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, uyBulogoKomokPaymentId);
                query.AddAttribute(assignSrc, "MembershipType");
                query.AddAttribute(assignSrc, "EmploymentStatus");
                query.AddAttribute(appSrc, "RegNo");
                var table = new DataTable();
                using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                {
                    string s = "";
                    while (reader.Read())
                    {
                        var famMemberType = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                        var empStatus = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                        var sNum = sectionTypeId == section1TypeId ? 1 : 2;
                        var field = orderTypeName + "Section" + sNum + "AppCount";
                        var appNo = reader.IsDbNull(2) ? "---" : reader.GetString(2);
                        if (empStatus == empStatusTo3Id || empStatus == empStatusTo16Id)
                        {
                            item1[field] = ((int?)item1[field] ?? 0) + 1;
                            if (famMemberType == famMemberTypeId1)
                            {
                                item2[field] = ((int?)item2[field] ?? 0) + 1;
                            }
                            else if (famMemberType == famMemberTypeId2)
                            {
                                item3[field] = ((int?)item3[field] ?? 0) + 1;
                                s += "; " + appNo;
                            }
                            else if (famMemberType == famMemberTypeId3)
                            {
                                item4[field] = ((int?)item4[field] ?? 0) + 1;
                            }
                        }
                        else
                        {

                            var statusName = empStatus != Guid.Empty ? context.Enums.GetValue(empStatus).Value : "Род занятий не указан!";
                            throw new Exception(string.Format("Не могу сформировать заявку. У заявления с номером \"{0}\" у получателя указано некорректно род занятий \"{1}\". Просьба проверить и исправить данное значение, и повторить сформировать заново.", appNo, statusName));
                        }
                    }
                    //throw new Exception(s);
                }
                if (orderDefId != bankOrderDefId) CalcAppCount(userId, bankOrderDefId, year, month, orgId, item1, item2, item3, item4, "Bank", postPercent, context, sectionTypeId);
            }

            private static void CalcOrdersCount(Guid userId, Guid orderDefId, int year, int month, Guid orgId, Doc item1, string orderTypeName, WorkflowContext context)
            {
                SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                var orderSrc = query.JoinSource(query.Source, orderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, section2TypeId);
                query.AddCondition(ExpressionOperation.And, orderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                query.AddCondition(ExpressionOperation.And, orderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, uyBulogoKomokPaymentId);
                query.AddAttribute(orderSrc, "&Id", SqlQuerySummaryFunction.Count);
                var table = new DataTable();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    if (reader.Read())
                    {
                        int docCount = reader.IsDbNull(0) ? 0 : reader.GetInt32(0);
                        item1[orderTypeName + "Section2DocCount"] = docCount;
                    }
                }
                if (orderDefId != bankOrderDefId)
                    CalcOrdersCount(userId, bankOrderDefId, year, month, orgId, item1, "Bank", context);
            }

            private static void CalcAmountSum(Guid userId, int year, int month, Guid orgId, Doc item1, Doc item2, Doc item3, Doc item4, decimal postPercent, string orderTypeName, WorkflowContext context, Guid sectionTypeId)
            {
                var field = orderTypeName + "Section" + (sectionTypeId == section1TypeId ? 1 : 2);
                if (orderTypeName == "Post")
                {
                    SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                    var orderSrc = query.JoinSource(query.Source, postOrderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                    var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                    var assignSrc = query.JoinSource(appSrc, assignmentDefId, SqlSourceJoinType.Inner, "Assignments");
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                    query.AddCondition(ExpressionOperation.And, postOrderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                    query.AddCondition(ExpressionOperation.And, postOrderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                    query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, uyBulogoKomokPaymentId);
                    query.AddAttribute(assignSrc, "Amount");
                    query.AddAttribute(assignSrc, "MembershipType");
                    query.AddAttribute(assignSrc, "EmploymentStatus");
                    var table = new DataTable();
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    foreach (DataRow row in table.Rows)
                    {
                        decimal amount = row[0] is DBNull ? 0 : (decimal)row[0];
                        var memberTypeAssign = row[1] is DBNull ? Guid.Empty : (Guid)row[1];
                        var empStatus = row[2] is DBNull ? Guid.Empty : (Guid)row[2];
                        decimal service = (amount / 100) * postPercent;
                        if (empStatus == empStatusTo3Id || empStatus == empStatusTo16Id)
                        {
                            item1[field + "NeedAmount"] = ((decimal?)item1[field + "NeedAmount"] ?? 0m) + amount;
                            item1[field + "Percent"] = ((decimal?)item1[field + "Percent"] ?? 0m) + service;
                            if (memberTypeAssign == famMemberTypeId1)
                            {
                                item2[field + "NeedAmount"] = ((decimal?)item2[field + "NeedAmount"] ?? 0m) + amount;
                                item2[field + "Percent"] = ((decimal?)item2[field + "Percent"] ?? 0m) + service;
                            }
                            else if (memberTypeAssign == famMemberTypeId2)
                            {
                                item3[field + "NeedAmount"] = ((decimal?)item3[field + "NeedAmount"] ?? 0m) + amount;
                                item3[field + "Percent"] = ((decimal?)item3[field + "Percent"] ?? 0m) + service;
                            }
                            else if (memberTypeAssign == famMemberTypeId3)
                            {
                                item4[field + "NeedAmount"] = ((decimal?)item4[field + "NeedAmount"] ?? 0m) + amount;
                                item4[field + "Percent"] = ((decimal?)item4[field + "Percent"] ?? 0m) + service;
                            }
                        }
                    }
                    CalcAmountSum(userId, year, month, orgId, item1, item2, item3, item4, postPercent, "Bank", context, sectionTypeId);
                }
                else
                {
                    SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                    var orderSrc = query.JoinSource(query.Source, bankOrderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                    var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                    var assignSrc = query.JoinSource(appSrc, assignmentDefId, SqlSourceJoinType.Inner, "Assignments");
                    var bankSrc = query.JoinSource(orderSrc, bankDefId, SqlSourceJoinType.Inner, "Bank");
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                    query.AddCondition(ExpressionOperation.And, bankOrderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                    query.AddCondition(ExpressionOperation.And, bankOrderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                    query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, uyBulogoKomokPaymentId);
                    query.AddAttribute(assignSrc, "Amount");
                    query.AddAttribute(assignSrc, "MembershipType");
                    query.AddAttribute(assignSrc, "EmploymentStatus");
                    var bankAttr = query.AddAttribute(bankSrc, "Percent");
                    query.AddAttribute(appSrc, "RegNo");
                    //query.AddGroupAttribute(bankAttr);
                    var table = new DataTable();
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    string s = "";
                    foreach (DataRow row in table.Rows)
                    {
                        decimal amount = row[0] is DBNull ? 0 : (decimal)row[0];
                        string appNo = "";
                        if (sectionTypeId == section1TypeId)
                        {
                            appNo = row[4] is DBNull ? "" : row[4].ToString();
                            s += string.Format("{0}:{1}\n", appNo, amount);
                        }
                        var memberTypeAssignBank = row[1] is DBNull ? Guid.Empty : (Guid)row[1];
                        var empStatus = row[2] is DBNull ? Guid.Empty : (Guid)row[2];
                        double percent;
                        double.TryParse(row[3].ToString(), out percent);
                        decimal service = (amount / 100) * (decimal)percent;
                        if (empStatus == empStatusTo3Id || empStatus == empStatusTo16Id)
                        {
                            item1[field + "NeedAmount"] = ((decimal?)item1[field + "NeedAmount"] ?? 0m) + amount;
                            item1[field + "Percent"] = ((decimal?)item1[field + "Percent"] ?? 0m) + service;
                            if (memberTypeAssignBank == famMemberTypeId1)
                            {
                                item2[field + "NeedAmount"] = ((decimal?)item2[field + "NeedAmount"] ?? 0m) + amount;
                                item2[field + "Percent"] = ((decimal?)item2[field + "Percent"] ?? 0m) + service;
                            }
                            else if (memberTypeAssignBank == famMemberTypeId2)
                            {
                                item3[field + "NeedAmount"] = ((decimal?)item3[field + "NeedAmount"] ?? 0m) + amount;
                                item3[field + "Percent"] = ((decimal?)item3[field + "Percent"] ?? 0m) + service;
                            }
                            else if (memberTypeAssignBank == famMemberTypeId3)
                            {
                                item4[field + "NeedAmount"] = ((decimal?)item4[field + "NeedAmount"] ?? 0m) + amount;
                                item4[field + "Percent"] = ((decimal?)item4[field + "Percent"] ?? 0m) + service;
                            }
                        }
                    }
                    //throw new Exception(s);
                }
            }

            public static decimal GetPostPercent(int year, int month, Guid userId, WorkflowContext context)
            {
                SqlQuery query = new SqlQuery(context.DataContext, postDefId, userId);
                query.AddCondition(ExpressionOperation.And, postDefId, "DateFrom", ConditionOperation.LessEqual, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, postDefId, "DateTo", ConditionOperation.GreatEqual, new DateTime(year, month, DateTime.DaysInMonth(year, month)));
                query.AddAttribute("Size");
                using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    if (reader.Read()) return !reader.Reader.IsDBNull(0) ? reader.GetDecimal(0) : 0;
                return 0m;
            }
        }

        public static class PrivilegePaymentReport1
        {
            // Виды выплат                             
            private static readonly Guid privilege46PaymentId = new Guid("{7BEFD6DA-042C-4A77-90F3-A4424033E4DD}");//Денежная компенсация взамен льгот
            private static readonly Guid rehabSinglePaymentId = new Guid("{034D9A01-2B4A-481C-B7FD-A3255C27400C}");//Единовременная выплата реабилитированным гражданам
                                                                                                                   //Categories
            private static Guid category1 = new Guid("{2E662509-144D-47CA-9F2B-087BB6F4AE91}");//Участник ликвидации последствий аварии на ЧАЭЗ 1989 г.
            private static Guid category2 = new Guid("{DF92F99F-BA8D-415C-8236-F54206AF0F73}");//Участник ликвидации последствий аварии на ЧАЭЗ 1988 г.
            private static Guid category3 = new Guid("{8691D5FB-7611-4AAA-AD84-60D2AADF50BE}");//Участник ликвидации последствий аварии на ЧАЭЗ 1986-87 гг.
            private static Guid category4 = new Guid("{63DFD63A-2868-411F-A52F-039E37B1D39F}");//Участник ВОВ, награжденный орденами славы 3-х степеней
            private static Guid category5 = new Guid("{677A9F5A-79F3-43F1-948E-AE10F6007677}");//Участник ВОВ
            private static Guid category6 = new Guid("{7EE64986-5A47-49D0-A288-88A537E6D0A0}");//Участник боевых действий на территории других государств
            private static Guid category7 = new Guid("{145C4F38-9BCF-44B3-95D0-EA1B8D1B869B}");//Участник блокады г.Ленинграда
            private static Guid category8 = new Guid("{8C21641E-1AC2-438A-ABDB-D6D17D195BB5}");//Семьям погибших военнослужащих при исполнении обязанностей военной службы
            private static Guid category9 = new Guid("{ECD8E6DF-3C3E-427D-BDC1-58E132A8B03D}");//Семья, потерявшая кормильца - участника ЧАЭС
            private static Guid category10 = new Guid("{93B5323F-E16B-4A76-AEB6-7CE166B0471B}");//Семья погибшего/пропавшего без вести в ВОВ военнослужащего
            private static Guid category11 = new Guid("{B038B840-021C-436E-B34D-4EB0E90DB767}");//Семьи сотрудников МВД, погибших при исполнении сл. обяз. или сл.долга, умерших после увольн. вслед-е ран./трав./забол., получ. в пер.прохожд.службы
            private static Guid category12 = new Guid("{C5728DEE-CE74-41C2-A0CF-B1182615D6B3}");//Реабилитированный, пострадавший в результате репрессий
            private static Guid category13 = new Guid("{24D5389C-9102-4369-A978-E2EF7DFC36C8}");//Почетный донор
            private static Guid category14 = new Guid("{C8B1465C-F377-4D9B-B23C-04FFC6836597}");//Несовершеннолетний узник концлагерей, гетто
            private static Guid category15 = new Guid("{FA1787EF-A156-400E-8CA3-436203D3C322}");//ЛОВЗ Сов.Армии 3 гр. (во время несения службы)
            private static Guid category16 = new Guid("{22CB5768-069B-4E18-9F4C-D3F308AF4E3F}");//ЛОВЗ Сов.Армии 2 гр. (во время несения службы)
            private static Guid category17 = new Guid("{1EA3FA92-E895-4A9D-AD5F-72FE1EE3D5BF}");//ЛОВЗ Сов.Армии 1 гр. (во время несения службы)
            private static Guid category18 = new Guid("{CBD190B1-44AD-4E6B-8B79-34BF60619CF5}");//ЛОВЗ по слуху и зрению до 18 лет
            private static Guid category19 = new Guid("{DB4E8C78-A17F-44FD-890D-A67F1EA2B457}");//ЛОВЗ 3 гр. по слуху
            private static Guid category21 = new Guid("{18A54BA7-0330-40B3-AE9E-67D049004AB0}");//ЛОВЗ 2 гр. по слуху
            private static Guid category23 = new Guid("{C7B397DF-8325-4F0D-A94C-F8BD5E8152E4}");//ЛОВЗ 1 гр. по слуху
            private static Guid category20 = new Guid("{5EE97C99-B2A1-4759-9A28-876B08FE7BA8}");//ЛОВЗ 3 гр. по зрению
            private static Guid category22 = new Guid("{5EE97C99-B2A1-4759-9A28-876B08FE7BA8}");//ЛОВЗ 2 гр. по зрению
            private static Guid category24 = new Guid("{5EE97C99-B2A1-4759-9A28-876B08FE7BA8}");//ЛОВЗ 1 гр. по зрению
            private static Guid category25 = new Guid("{A4B6BC13-479B-468F-B4EB-D661417B1127}");//Лица, награжденные за работу/службу в тылу в годы ВОВ с группой инв.
            private static Guid category26 = new Guid("{891911A4-C7C9-4E8D-919B-6AE2B7DE1AB7}");//Лица, награжденные за работу/службу в тылу в годы ВОВ без группы инв-ти
            private static Guid category27 = new Guid("{76668EA1-47E2-4F09-AD73-4AF24BF0D55C}");//Лица заболевшие лучевой болезнью (ЧАЭС)
            private static Guid category28 = new Guid("{E14662C0-719D-4B26-886F-55C8BD43A11E}");//Инвалид-реабилитиров., пострадавший в результате репрессий
            private static Guid category29 = new Guid("{BFE0BD8C-032D-4DF2-ACC1-60F2385925E9}");//Инвалид Советской армии при исполнении служеб. Обязанности
            private static Guid category30 = new Guid("{62960950-7E8F-420F-BCE6-D7F92CA4EA93}");//Инвалид ВОВ 3 группы
            private static Guid category31 = new Guid("{626357F4-5133-4426-A712-2E785E556F6E}");//Инвалид ВОВ 2 группы
            private static Guid category32 = new Guid("{32B76FDF-145B-492C-A850-BE903B7AB6CA}");//Инвалид ВОВ 1 группы
            private static Guid category33 = new Guid("{7CC482B7-A2FD-4B2E-8BFE-9CF0B96D8E05}");//Инв.3 гр.-участник ликвидации последствий аварии на ЧАЭC
            private static Guid category35 = new Guid("{69E2D977-7C5A-4315-BF6A-6E790D19D7EC}");//Инв.2 гр.-участник ликвидации последствий аварии на ЧАЭC
            private static Guid category38 = new Guid("{0A8D29A0-4806-422B-89EF-8F11AE835098}");//Инв.1 гр. - участник ликвидации последствий аварии на ЧАЭC
            private static Guid category34 = new Guid("{8828EAF5-5884-4BB0-B097-3D65496E4168}");//Инв.3 гр.-участник боевых действий на террит.др.государств
            private static Guid category36 = new Guid("{EB091676-B785-42AC-A330-44DFFBFDE1F6}");//Инв.2 гр.-участник боевых действий на террит.др.государств
            private static Guid category37 = new Guid("{3F0DD390-EFF5-42BA-A6D4-BB600F61F793}");//Инв.1 гр.-участник боевых действий на террит.др.государств
            private static Guid category39 = new Guid("{DCAC340F-4132-4514-B90C-581B2B24E37D}");//Дети участника ЧАЭС в возрасте до 18 лет
            private static Guid category40 = new Guid("{2222FD98-B885-4DC0-A0D4-271600AF281A}");//Дети ВИЧ-инфиц. или больные СПИДом
            private static Guid category41 = new Guid("{B0004A56-4831-4CB8-B12B-C39280493A0B}");//Гражданин, мобилизованный в трудовую армию в годы ВОВ
            private static Guid category42 = new Guid("{A1FD4E39-07A0-4117-A99F-1CBFB9BF9E97}");//Герой СССР, герой КР, награжденный за боевые заслуги
            private static Guid category43 = new Guid("{95EC8C3C-0006-45E6-9A24-0FBA212117AA}");//Герой соц. труда, кавалер ордена трудовой славы 3-х степеней
            private static Guid category44 = new Guid("{92F076ED-8558-416F-B28C-AD80017C8D56}");//Вдова(ец) участ. Блокады Ленинграда
            private static Guid category45 = new Guid("{5F257499-9BF5-4850-AC3B-02264CDB62C8}");//Вдова(ец) УОВ
            private static Guid category46 = new Guid("{93933D4F-DE39-4C43-839F-6BE09DF1C9C5}");//Вдова(ец) умершего ИОВ

            private static List<object> categoryTypes = new List<object>
            {
            category1,category2,category3,category4,category5,category6,category7,category8,category9,category10,category11,
            category12,category13,category14,category15,category16,category17,category18,category19,category20,category21,category22,category23,
            category24,category25,category26,category27,category28,category29,category30,category31,category32,category33,category34,category35,
            category36,category37,category38,category39,category40,category41,category42,category43,category44,category45,category46
            };
            // Document Defs Id 1e750c67-2ddf-488e-a4c4-d9454743306;
            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");// "{04D25808-6DE9-42F5-8855-6F68A94A224C}"
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{4447EA34-67AB-46F2-BE03-A406CAC4EABC}");
            private static readonly Guid reportItemDefId = new Guid("{66605D33-A39E-4709-8534-C1505C041182}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");
            // States
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid canceledStateId = new Guid("{C65E63CC-54E9-424E-AFDC-5A8DB1CB04A3}"); // Аннулирован
            private static readonly Guid onApprovingStateId = new Guid("{5CD9E88D-671E-4A44-AD92-9F74DA3B47F7}"); //На утверждении
            private static readonly Guid onRegisteringStateId = new Guid("{A9CD37C4-A718-4DE1-9E95-EC8EC280C8D4}"); // На регистрации
            private static readonly Guid refusedStateId = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}"); // Отказан

            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {
                try
                {
                    var ui = context.GetUserInfo();
                    return Build(year, month, context.UserId, (Guid)ui.OrganizationId, context);

                }

                catch (Exception ex)
                {
                    throw new ApplicationException("TargetSite: " + ex.TargetSite.Name + "; Message: " + ex.Message);
                }

            }

            private static Doc GetReportDoc(WorkflowContext context, List<object[]> reports)
            {
                Doc report;
                var docRepo = context.Documents;
                var approvedReportId = reports.FirstOrDefault(r => (Guid)r[1] == approvedStateId);
                if (approvedReportId != null)
                {
                    context["ApprovedReport"] = docRepo.LoadById((Guid)approvedReportId[0]);
                    return null;
                }
                reports = reports.Where(r => (Guid)r[1] == onRegisteringStateId).ToList();
                if (reports.Count > 0)
                {
                    var onRegisteringReportId = reports.FirstOrDefault(x => (DateTime)x[2] == reports.Max(r => (DateTime)r[2]));
                    report = docRepo.LoadById((Guid)onRegisteringReportId[0]);
                    reports.RemoveAll(x => (Guid)x[0] == (Guid)onRegisteringReportId[0]);
                    report["DocCount"] = null;
                    report["AppCount"] = null;
                    report["NeedAmount"] = null;
                    report["Percent"] = null;
                    report["NeedAmountPercent"] = null;
                    docRepo.ClearAttrDocList(report.Id, report.Get<DocListAttribute>("Rows").AttrDef.Id);
                    foreach (var reportId in reports.Select(x => (Guid)x[0]))
                    {
                        docRepo.SetDocState(reportId, refusedStateId);
                    }
                }
                else
                    report = docRepo.New(reportDefId);
                return report;
            }

            static List<ReportItem> InitReport(WorkflowContext context, Doc report, List<ReportItem> reportItemList)
            {
                var reportItems = new List<Doc>();
                var docRepo = context.Documents;
                foreach (var rItemId in docRepo.DocAttrList(out int c, report, "Rows", 0, 0))
                {
                    var curDocRepo = docRepo.LoadById(rItemId);
                    if (categoryTypes.Contains((Guid)curDocRepo["Category"]))
                        reportItems.Add(curDocRepo);
                }
                CalcFields(reportItems, report, reportItemList);
                return reportItemList;
            }

            public static List<ReportItem> Build(int year, int month, Guid userId, Guid orgId, WorkflowContext context)
            {
                if (year < 2011 || year > 3000)
                    throw new ApplicationException("Ошибка в значении года!");
                if (month < 1 || month > 12)
                    throw new ApplicationException("Ошибка в значении месяца!");
                var docRepo = context.Documents;
                var qb = new QueryBuilder(reportDefId);
                qb.Where("&OrgId").Eq(orgId).And("Month").Eq(month).And("Year").Eq(year).And("&State").Neq(refusedStateId);

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("&Id");
                query.AddAttribute("&State");
                query.AddAttribute("&Created");
                var reports = new List<object[]>();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        reports.Add(new object[] { reader.GetGuid(0), reader.GetGuid(1), reader.GetDateTime(2) });
                    }
                }

                Dictionary<Guid, Decimal> prices = GetPriceList(userId, month, year, context);

                decimal postPercent = GetPostPercent(year, month, userId, context);
                List<Doc> items = new List<Doc>();
                var reportItemList = new List<ReportItem>();
                //Инициализация строк отчета
                foreach (Guid categoryId in categoryTypes)
                {
                    if (!prices.ContainsKey(categoryId)) continue;
                    Doc item = docRepo.New(reportItemDefId);

                    item["Category"] = categoryId;
                    item["PaymentPrice"] = prices[categoryId];
                    reportItemList.Add(new ReportItem { Category = context.Enums.GetValue(categoryId).Value, PaymentPrice = prices[categoryId] });
                    items.Add(item);
                }

                Doc report = GetReportDoc(context, reports);
                if (report == null)
                {
                    return InitReport(context, context.Get<Doc>("ApprovedReport"), reportItemList);
                }

                CalcSection1(items, context, year, month, orgId, userId, postPercent);

                CalcSection2(items, context, year, month, orgId, userId, postPercent);

                CalcFields(items, report, reportItemList);

                items.Reverse();
                foreach (Doc item in items)
                {
                    docRepo.Save(item);
                    docRepo.AddDocToList(item.Id, report, "Rows");
                }
                report["Year"] = year;
                report["Month"] = month;
                report["Organization"] = orgId;

                docRepo.Save(report);
                docRepo.SetDocState(report, onRegisteringStateId);
                return reportItemList;
            }

            private static void CalcSection1(List<Doc> items, WorkflowContext context, int year, int month, Guid orgId, Guid userId, decimal postPercent)
            {
                //CalcAppCount(userId, postOrderDefId, year, month, orgId, items, "Post", postPercent, context, section1TypeId);
                //CalcSection1Months(userId, postOrderDefId, year, month, orgId, items, "Post", context);
                CalcAmountSum(userId, year, month, orgId, items, postPercent, "Post", context, section1TypeId);
            }

            private static void CalcSection2(List<Doc> items, WorkflowContext context, int year, int month, Guid orgId, Guid userId, decimal postPercent)
            {
                CalcAppCount(userId, postOrderDefId, year, month, orgId, items, "Post", postPercent, context, section2TypeId);
                CalcOrdersCount(userId, postOrderDefId, year, month, orgId, items, "Post", context);
                CalcAmountSum(userId, year, month, orgId, items, postPercent, "Post", context, section2TypeId);
            }

            private static void CalcFields(List<Doc> items, Doc report, List<ReportItem> _items)
            {
                int i = 0;
                foreach (var item in items)
                {
                    var _item = _items[i];
                    FieldsInit(item);
                    item["PostNeedAmount"] = (decimal)item["PostSection1NeedAmount"] + (decimal)item["PostSection2NeedAmount"];
                    item["PostPercent"] = (decimal)item["PostSection1Percent"] + (decimal)item["PostSection2Percent"];
                    item["PostNeedAmountPercent"] = (decimal)item["PostNeedAmount"] + (decimal)item["PostPercent"];

                    item["BankNeedAmount"] = (decimal)item["BankSection1NeedAmount"] + (decimal)item["BankSection2NeedAmount"];
                    item["BankPercent"] = (decimal)item["BankSection1Percent"] + (decimal)item["BankSection2Percent"];
                    item["BankNeedAmountPercent"] = (decimal)item["BankNeedAmount"] + (decimal)item["BankPercent"];

                    item["DocCount"] = (int)item["PostSection2DocCount"] + (int)item["BankSection2DocCount"];
                    item["AppCount"] = (int)item["PostSection2AppCount"] + (int)item["BankSection2AppCount"];
                    item["NeedAmount"] = (decimal)item["PostNeedAmount"] + (decimal)item["BankNeedAmount"];
                    item["Percent"] = (decimal)item["PostPercent"] + (decimal)item["BankPercent"];
                    item["NeedAmountPercent"] = (decimal)item["PostNeedAmountPercent"] + (decimal)item["BankNeedAmountPercent"];

                    report["DocCount"] = ((int?)report["DocCount"] ?? 0) + (int)item["DocCount"];
                    report["AppCount"] = ((int?)report["AppCount"] ?? 0) + (int)item["AppCount"];
                    report["NeedAmount"] = ((decimal?)report["NeedAmount"] ?? 0) + (decimal)item["NeedAmount"];
                    report["Percent"] = ((decimal?)report["Percent"] ?? 0) + (decimal)item["Percent"];
                    report["NeedAmountPercent"] = ((decimal?)report["NeedAmountPercent"] ?? 0) + (decimal)item["NeedAmountPercent"];
                    FieldsInit(item, _item);
                    i++;
                }
            }

            private static void FieldsInit(Doc item, ReportItem _item = null)
            {
                item["DocCount"] = (int?)item["DocCount"] ?? 0;
                item["AppCount"] = (int?)item["AppCount"] ?? 0;
                item["NeedAmount"] = (decimal?)item["NeedAmount"] ?? 0;
                item["Percent"] = (decimal?)item["Percent"] ?? 0;
                item["NeedAmountPercent"] = (decimal?)item["NeedAmountPercent"] ?? 0;
                item["PostSection1MonthCount"] = (int?)item["PostSection1MonthCount"] ?? 0;
                item["PostSection1AppCount"] = (int?)item["PostSection1AppCount"] ?? 0;
                item["PostSection1NeedAmount"] = (decimal?)item["PostSection1NeedAmount"] ?? 0;
                item["PostSection1Percent"] = (decimal?)item["PostSection1Percent"] ?? 0;
                item["PostSection2DocCount"] = (int?)item["PostSection2DocCount"] ?? 0;
                item["PostSection2AppCount"] = (int?)item["PostSection2AppCount"] ?? 0;
                item["PostSection2NeedAmount"] = (decimal?)item["PostSection2NeedAmount"] ?? 0;
                item["PostSection2Percent"] = (decimal?)item["PostSection2Percent"] ?? 0;
                item["PostAppCount"] = (int?)item["PostAppCount"] ?? 0;
                item["PostNeedAmount"] = (decimal?)item["PostNeedAmount"] ?? 0;
                item["PostPercent"] = (decimal?)item["PostPercent"] ?? 0;
                item["PostNeedAmountPercent"] = (decimal?)item["PostNeedAmountPercent"] ?? 0;

                item["BankSection1MonthCount"] = (int?)item["BankSection1MonthCount"] ?? 0;
                item["BankSection1AppCount"] = (int?)item["BankSection1AppCount"] ?? 0;
                item["BankSection1NeedAmount"] = (decimal?)item["BankSection1NeedAmount"] ?? 0;
                item["BankSection1Percent"] = (decimal?)item["BankSection1Percent"] ?? 0;
                item["BankSection2DocCount"] = (int?)item["BankSection2DocCount"] ?? 0;
                item["BankSection2AppCount"] = (int?)item["BankSection2AppCount"] ?? 0;
                item["BankSection2NeedAmount"] = (decimal?)item["BankSection2NeedAmount"] ?? 0;
                item["BankSection2Percent"] = (decimal?)item["BankSection2Percent"] ?? 0;
                item["BankAppCount"] = (int?)item["BankAppCount"] ?? 0;
                item["BankNeedAmount"] = (decimal?)item["BankNeedAmount"] ?? 0;
                item["BankPercent"] = (decimal?)item["BankPercent"] ?? 0;
                item["BankNeedAmountPercent"] = (decimal?)item["BankNeedAmountPercent"] ?? 0;
                if (_item != null)
                {
                    _item.DocCount = (int?)item["DocCount"] ?? 0;
                    _item.AppCount = (int?)item["AppCount"] ?? 0;
                    _item.NeedAmount = (decimal?)item["NeedAmount"] ?? 0;
                    _item.Percent = (decimal?)item["Percent"] ?? 0;
                    _item.NeedAmountPercent = (decimal?)item["NeedAmountPercent"] ?? 0;
                    _item.PostSection1MonthCount = (int?)item["PostSection1MonthCount"] ?? 0;
                    _item.PostSection1AppCount = (int?)item["PostSection1AppCount"] ?? 0;
                    _item.PostSection1NeedAmount = (decimal?)item["PostSection1NeedAmount"] ?? 0;
                    _item.PostSection1Percent = (decimal?)item["PostSection1Percent"] ?? 0;
                    _item.PostSection2DocCount = (int?)item["PostSection2DocCount"] ?? 0;
                    _item.PostSection2AppCount = (int?)item["PostSection2AppCount"] ?? 0;
                    _item.PostSection2NeedAmount = (decimal?)item["PostSection2NeedAmount"] ?? 0;
                    _item.PostSection2Percent = (decimal?)item["PostSection2Percent"] ?? 0;
                    _item.PostAppCount = (int?)item["PostAppCount"] ?? 0;
                    _item.PostNeedAmount = (decimal?)item["PostNeedAmount"] ?? 0;
                    _item.PostPercent = (decimal?)item["PostPercent"] ?? 0;
                    _item.PostNeedAmountPercent = (decimal?)item["PostNeedAmountPercent"] ?? 0;

                    _item.BankSection1MonthCount = (int?)item["BankSection1MonthCount"] ?? 0;
                    _item.BankSection1AppCount = (int?)item["BankSection1AppCount"] ?? 0;
                    _item.BankSection1NeedAmount = (decimal?)item["BankSection1NeedAmount"] ?? 0;
                    _item.BankSection1Percent = (decimal?)item["BankSection1Percent"] ?? 0;
                    _item.BankSection2DocCount = (int?)item["BankSection2DocCount"] ?? 0;
                    _item.BankSection2AppCount = (int?)item["BankSection2AppCount"] ?? 0;
                    _item.BankSection2NeedAmount = (decimal?)item["BankSection2NeedAmount"] ?? 0;
                    _item.BankSection2Percent = (decimal?)item["BankSection2Percent"] ?? 0;
                    _item.BankAppCount = (int?)item["BankAppCount"] ?? 0;
                    _item.BankNeedAmount = (decimal?)item["BankNeedAmount"] ?? 0;
                    _item.BankPercent = (decimal?)item["BankPercent"] ?? 0;
                    _item.BankNeedAmountPercent = (decimal?)item["BankNeedAmountPercent"] ?? 0;
                }
            }

            public class ReportItem
            {
                public int RowNo { get; set; }
                public string RowName { get; set; }
                public decimal Percent { get; set; }
                public string Category { get; set; }
                public int AppCount { get; set; }
                public decimal NeedAmountPercent { get; set; }
                public decimal PaymentPrice { get; set; }
                public decimal NeedAmount { get; set; }
                public int DocCount { get; set; }
                public int PostAppCount { get; set; }
                public decimal PostPercent { get; set; }
                public decimal PostNeedAmountPercent { get; set; }
                public decimal PostNeedAmount { get; set; }
                public int PostDocCount { get; set; }
                public int PostSection1DocCount { get; set; }
                public int PostSection1AppCount { get; set; }
                public decimal PostSection1Percent { get; set; }
                public decimal PostSection1NeedAmountPercent { get; set; }
                public int PostSection1MonthCount { get; set; }
                public decimal PostSection1NeedAmount { get; set; }
                public int PostSection2AppCount { get; set; }
                public int PostSection2MonthCount { get; set; }
                public decimal PostSection2NeedAmountPercent { get; set; }
                public decimal PostSection2Percent { get; set; }
                public int BankAppCount { get; set; }
                public int PostSection2DocCount { get; set; }
                public decimal PostSection2NeedAmount { get; set; }
                public int BankDocCount { get; set; }
                public decimal BankNeedAmountPercent { get; set; }
                public decimal BankPercent { get; set; }
                public decimal BankNeedAmount { get; set; }
                public int BankSection2DocCount { get; set; }
                public decimal BankSection2NeedAmount { get; set; }
                public decimal BankSection2NeedAmountPercent { get; set; }
                public int BankSection2MonthCount { get; set; }
                public decimal BankSection2Percent { get; set; }
                public int BankSection2AppCount { get; set; }
                public decimal BankSection1NeedAmount { get; set; }
                public int BankSection1DocCount { get; set; }
                public decimal BankSection1Percent { get; set; }
                public decimal BankSection1NeedAmountPercent { get; set; }
                public int BankSection1AppCount { get; set; }
                public int BankSection1MonthCount { get; set; }
            }

            private static void CalcAppCount(Guid userId, Guid orderDefId, int year, int month, Guid orgId, List<Doc> items, string orderTypeName, decimal postPercent, WorkflowContext context, Guid sectionTypeId)
            {
                SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                var orderSrc = query.JoinSource(query.Source, orderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                //query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                query.AddCondition(ExpressionOperation.And, orderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                query.AddCondition(ExpressionOperation.And, orderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, privilege46PaymentId);
                query.AddAttribute(appSrc, "Assignments", SqlQuerySummaryFunction.Count);
                var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                query.AddGroupAttribute(appPaymentAttr);
                var table = new DataTable();
                using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }

                foreach (DataRow row in table.Rows)
                {
                    int count = row[0] is DBNull ? 0 : (int)row[0];
                    Guid categoryId;
                    Guid.TryParse(row[1].ToString(), out categoryId);
                    if (categoryId == Guid.Empty)
                        throw new ApplicationException("Ошибка при чтении кол-ва чел. - " + orderTypeName + ". Категория не найдена.");

                    if (!categoryTypes.Contains(categoryId))
                    {
                        var catName = context.Enums.GetValue(categoryId).Value;
                        throw new ApplicationException("Ошибка при чтении кол-ва чел. - " + orderTypeName + ". Категория \"" + catName + "\" не входит в число получателей денежной компенсации.");
                    }
                    var item = items[categoryTypes.IndexOf(categoryId)];
                    var sNum = sectionTypeId == section1TypeId ? 1 : 2;
                    var field = orderTypeName + "Section" + sNum + "AppCount";
                    item[field] = ((int?)item[field] ?? 0) + count;
                }
                if (orderDefId != bankOrderDefId) CalcAppCount(userId, bankOrderDefId, year, month, orgId, items, "Bank", postPercent, context, sectionTypeId);
            }

            private static void CalcOrdersCount(Guid userId, Guid orderDefId, int year, int month, Guid orgId, List<Doc> items, string orderTypeName, WorkflowContext context)
            {
                SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                var orderSrc = query.JoinSource(query.Source, orderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, section2TypeId);
                query.AddCondition(ExpressionOperation.And, orderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                query.AddCondition(ExpressionOperation.And, orderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, privilege46PaymentId);
                query.AddAttribute(orderSrc, "&Id", SqlQuerySummaryFunction.Count);
                var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                query.AddGroupAttribute(appPaymentAttr);
                var table = new DataTable();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }
                foreach (DataRow row in table.Rows)
                {
                    int docCount = (int?)row[0] ?? 0;
                    Guid categoryId;
                    Guid.TryParse(row[1].ToString(), out categoryId);
                    if (categoryId == Guid.Empty) continue;
                    Doc item = items[categoryTypes.IndexOf(categoryId)];
                    item[orderTypeName + "Section2DocCount"] = docCount;
                }
                if (orderDefId != bankOrderDefId)
                    CalcOrdersCount(userId, bankOrderDefId, year, month, orgId, items, "Bank", context);
            }

            private static void CalcAmountSum(Guid userId, int year, int month, Guid orgId, List<Doc> items, decimal postPercent, string orderTypeName, WorkflowContext context, Guid sectionTypeId)
            {
                var field = orderTypeName + "Section" + (sectionTypeId == section1TypeId ? 1 : 2);
                if (orderTypeName == "Post")
                {
                    SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                    var orderSrc = query.JoinSource(query.Source, postOrderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                    var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                    query.AddCondition(ExpressionOperation.And, postOrderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                    query.AddCondition(ExpressionOperation.And, postOrderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                    query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, privilege46PaymentId);
                    query.AddAttribute("Amount", SqlQuerySummaryFunction.Sum);
                    var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                    query.AddGroupAttribute(appPaymentAttr);
                    var table = new DataTable();
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }

                    foreach (DataRow row in table.Rows)
                    {
                        decimal amount = row[0] is DBNull ? 0 : (decimal)row[0];
                        Guid categoryId;
                        Guid.TryParse(row[1].ToString(), out categoryId);
                        if (categoryId == Guid.Empty) continue;
                        try
                        {
                            decimal service = (amount / 100) * postPercent;
                            var item = items[categoryTypes.IndexOf(categoryId)];
                            item[field + "NeedAmount"] = ((decimal?)item[field + "NeedAmount"] ?? 0m) + amount;
                            item[field + "Percent"] = ((decimal?)item[field + "Percent"] ?? 0m) + service;
                        }
                        catch
                        {
                            continue;
                        }
                    }
                    CalcAmountSum(userId, year, month, orgId, items, postPercent, "Bank", context, sectionTypeId);
                }
                else
                {
                    SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                    var orderSrc = query.JoinSource(query.Source, bankOrderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                    var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                    var bankSrc = query.JoinSource(orderSrc, bankDefId, SqlSourceJoinType.Inner, "Bank");
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                    query.AddCondition(ExpressionOperation.And, bankOrderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                    query.AddCondition(ExpressionOperation.And, bankOrderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                    query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, privilege46PaymentId);
                    query.AddAttribute("Amount", SqlQuerySummaryFunction.Sum);
                    var bankAttr = query.AddAttribute(bankSrc, "Percent");
                    var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                    query.AddGroupAttribute(appPaymentAttr);
                    query.AddGroupAttribute(bankAttr);
                    var table = new DataTable();
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }

                    foreach (DataRow row in table.Rows)
                    {
                        decimal amount = row[0] is DBNull ? 0 : (decimal)row[0];
                        double percent = row[1] is DBNull ? 0 : (double)row[1];
                        Guid categoryId = row[2] is DBNull ? Guid.Empty : (Guid)row[2];
                        if (categoryId == Guid.Empty) continue;
                        try
                        {
                            decimal service = amount / 100 * (decimal)percent;

                            var item = items[categoryTypes.IndexOf(categoryId)];
                            item[field + "NeedAmount"] = ((decimal?)item[field + "NeedAmount"] ?? 0m) + amount;
                            item[field + "Percent"] = ((decimal?)item[field + "Percent"] ?? 0m) + service;
                        }
                        catch
                        {
                            continue;
                        }
                    }
                }
            }

            private static Dictionary<Guid, Decimal> GetPriceList(Guid userId, int month, int year, WorkflowContext context)
            {
                Dictionary<Guid, Decimal> catList = new Dictionary<Guid, Decimal>();
                QueryBuilder qb = new QueryBuilder(tariffDefId, userId);
                qb.Where("EffectiveDate").Le(new DateTime(year, month, 1)).And("ExpiryDate").Ge(new DateTime(year, month,
                DateTime.DaysInMonth(year, month)))
                .And("PaymentType").Eq(privilege46PaymentId);
                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("Category");
                query.AddAttribute("Amount");
                using (DataTable table = new DataTable())
                {
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    foreach (DataRow row in table.Rows)
                    {
                        Guid categoryId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                        if (categoryId != Guid.Empty)
                            if (!catList.ContainsKey(categoryId))
                                catList.Add(categoryId, row[1] is DBNull ? 0m : (decimal)row[1]);
                            else
                                throw new ApplicationException("Ошибка при чтении тарифов ДК. Сообщите администрации об этой ошибке.");
                    }
                }
                return catList;
            }

            public static decimal GetPostPercent(int year, int month, Guid userId, WorkflowContext context)
            {
                SqlQuery query = new SqlQuery(context.DataContext, postDefId, userId);
                query.AddCondition(ExpressionOperation.And, postDefId, "DateFrom", ConditionOperation.LessEqual, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, postDefId, "DateTo", ConditionOperation.GreatEqual, new DateTime(year, month, DateTime.DaysInMonth(year, month)));
                query.AddAttribute("Size");
                using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    if (reader.Read()) return !reader.Reader.IsDBNull(0) ? reader.GetDecimal(0) : 0;
                return 0m;
            }
        }

        public static class AdditionalAksySocialBenefitsReport1
        {
            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {

                try
                {
                    var ui = context.GetUserInfo();
                    return Build(year, month, context.UserId, (Guid)ui.OrganizationId, context);
                }
                catch (Exception ex)
                {
                    throw new ApplicationException(ex.TargetSite.Name + "; " + ex.Message);
                }
            }
            private static readonly Guid despAksyPaymentTypeId = new Guid("{E590688C-FE0E-4DE2-BEFC-35887CD23ABA}");
            // Document Defs
            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{73A632C4-9F82-4861-BF3A-E5C895A3B00F}");
            private static readonly Guid reportItemDefId = new Guid("{ED58934E-989C-4AE6-AB19-A432F06E43E0}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            // States
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid canceledStateId = new Guid("{C65E63CC-54E9-424E-AFDC-5A8DB1CB04A3}"); // Аннулирован
            private static readonly Guid onApprovingStateId = new Guid("{5CD9E88D-671E-4A44-AD92-9F74DA3B47F7}"); //На утверждении
            private static readonly Guid onRegisteringStateId = new Guid("{A9CD37C4-A718-4DE1-9E95-EC8EC280C8D4}"); // На регистрации
            private static readonly Guid refusedStateId = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}"); // Отказан

            private static readonly Guid categoryType1Id = new Guid("{12C16D1F-1AF5-4BB0-931A-2D1D9F07E1A5}");//Детям погибшего в событиях 17-18 марта 2002 года  в Аксыйском районе Джалал-Абадской области
            private static readonly Guid categoryType2Id = new Guid("{1BFDE4A6-D55F-435A-A34E-B77FAAEA265B}");//Родителям погибшего в событиях 17-18 марта 2002 года  в Аксыйском районе Джалал-Абадской области
            private static readonly Guid categoryType3Id = new Guid("{4F53C318-8590-453C-9A06-B7CD9DB29888}");//Вдовам погибшего в событиях 17-18 марта 2002 года  в Аксыйском районе Джалал-Абадской области
            private static readonly Guid categoryType4Id = new Guid("{E049D13F-DE27-4644-8473-E1F50F5C5611}");//Лицам, признанным инвалидами  в событиях 17-18 марта 2002 года  в Аксыйском районе Джалал-Абадской области
            private static readonly List<object> categoryTypes = new List<object>
            {
            categoryType1Id, categoryType2Id, categoryType3Id, categoryType4Id
            };

            private static Doc GetReportDoc(WorkflowContext context, List<object[]> reports)
            {
                Doc report;
                var docRepo = context.Documents;
                var approvedReportId = reports.FirstOrDefault(r => (Guid)r[1] == approvedStateId);
                if (approvedReportId != null)
                {
                    context["ApprovedReport"] = docRepo.LoadById((Guid)approvedReportId[0]);
                    return null;
                }
                reports = reports.Where(r => (Guid)r[1] == onRegisteringStateId).ToList();
                if (reports.Count > 0)
                {
                    var onRegisteringReportId = reports.FirstOrDefault(x => (DateTime)x[2] == reports.Max(r => (DateTime)r[2]));
                    report = docRepo.LoadById((Guid)onRegisteringReportId[0]);
                    reports.RemoveAll(x => (Guid)x[0] == (Guid)onRegisteringReportId[0]);
                    report["DocCount"] = null;
                    report["AppCount"] = null;
                    report["NeedAmount"] = null;
                    report["Percent"] = null;
                    report["NeedAmountPercent"] = null;
                    docRepo.ClearAttrDocList(report.Id, report.Get<DocListAttribute>("Rows").AttrDef.Id);
                    foreach (var reportId in reports.Select(x => (Guid)x[0]))
                    {
                        docRepo.SetDocState(reportId, refusedStateId);
                    }
                }
                else
                    report = docRepo.New(reportDefId);
                return report;
            }
            public static List<ReportItem> Build(int year, int month, Guid userId, Guid orgId, WorkflowContext context)
            {
                if (year < 2011 || year > 3000)
                    throw new ApplicationException("Ошибка в значении года!");
                if (month < 1 || month > 12)
                    throw new ApplicationException("Ошибка в значении месяца!");
                var docRepo = context.Documents;
                var qb = new QueryBuilder(reportDefId);
                qb.Where("&OrgId").Eq(orgId).And("Month").Eq(month).And("Year").Eq(year).And("&State").Neq(refusedStateId);

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("&Id");
                query.AddAttribute("&State");
                query.AddAttribute("&Created");
                var reports = new List<object[]>();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        reports.Add(new object[] { reader.GetGuid(0), reader.GetGuid(1), reader.GetDateTime(2) });
                    }
                }

                Dictionary<Guid, Decimal> prices = GetPriceList(userId, month, year, context);

                decimal postPercent = GetPostPercent(year, month, userId, context);
                List<Doc> items = new List<Doc>();
                var reportItemList = new List<ReportItem>();
                //Инициализация строк отчета
                foreach (Guid categoryId in categoryTypes)
                {
                    Doc item = docRepo.New(reportItemDefId);
                    item["Category"] = categoryId;
                    item["PaymentPrice"] = prices[categoryId];
                    reportItemList.Add(new ReportItem { Category = context.Enums.GetValue(categoryId).Value, PaymentPrice = prices[categoryId] });
                    items.Add(item);
                }

                Doc report = GetReportDoc(context, reports);
                if (report == null)
                {
                    return InitReport(context, context.Get<Doc>("ApprovedReport"), reportItemList);
                }

                CalcSection1(items, context, year, month, orgId, userId, postPercent);

                CalcSection2(items, context, year, month, orgId, userId, postPercent);

                CalcFields(items, report, reportItemList);

                items.Reverse();
                foreach (Doc item in items)
                {
                    docRepo.Save(item);
                    docRepo.AddDocToList(item.Id, report, "Rows");
                }
                report["Year"] = year;
                report["Month"] = month;
                report["Organization"] = orgId;

                docRepo.Save(report);
                docRepo.SetDocState(report, onRegisteringStateId);
                return reportItemList;
            }

            static List<ReportItem> InitReport(WorkflowContext context, Doc report, List<ReportItem> reportItemList)
            {
                var reportItems = new List<Doc>();
                var docRepo = context.Documents;
                foreach (var rItemId in docRepo.DocAttrList(out int c, report, "Rows", 0, 0))
                {
                    var curDocRepo = docRepo.LoadById(rItemId);
                    if (categoryTypes.Contains((Guid)curDocRepo["Category"]))
                        reportItems.Add(curDocRepo);
                }
                CalcFields(reportItems, report, reportItemList);
                return reportItemList;
            }

            private static void CalcSection1(List<Doc> items, WorkflowContext context, int year, int month, Guid orgId, Guid userId, decimal postPercent)
            {
                CalcAmountSum(userId, year, month, orgId, items, postPercent, "Post", context, section1TypeId);
            }

            private static void CalcSection2(List<Doc> items, WorkflowContext context, int year, int month, Guid orgId, Guid userId, decimal postPercent)
            {
                CalcAppCount(userId, postOrderDefId, year, month, orgId, items, "Post", postPercent, context, section2TypeId);
                CalcOrdersCount(userId, postOrderDefId, year, month, orgId, items, "Post", context);
                CalcAmountSum(userId, year, month, orgId, items, postPercent, "Post", context, section2TypeId);
            }

            private static void CalcFields(List<Doc> items, Doc report, List<ReportItem> _items)
            {
                int i = 0;
                foreach (var item in items)
                {
                    var _item = _items[i];
                    FieldsInit(item);
                    item["PostNeedAmount"] = (decimal)item["PostSection1NeedAmount"] + (decimal)item["PostSection2NeedAmount"];
                    item["PostPercent"] = (decimal)item["PostSection1Percent"] + (decimal)item["PostSection2Percent"];
                    item["PostNeedAmountPercent"] = (decimal)item["PostNeedAmount"] + (decimal)item["PostPercent"];

                    item["BankNeedAmount"] = (decimal)item["BankSection1NeedAmount"] + (decimal)item["BankSection2NeedAmount"];
                    item["BankPercent"] = (decimal)item["BankSection1Percent"] + (decimal)item["BankSection2Percent"];
                    item["BankNeedAmountPercent"] = (decimal)item["BankNeedAmount"] + (decimal)item["BankPercent"];

                    item["DocCount"] = (int)item["PostSection2DocCount"] + (int)item["BankSection2DocCount"];
                    item["AppCount"] = (int)item["PostSection2AppCount"] + (int)item["BankSection2AppCount"];
                    item["NeedAmount"] = (decimal)item["PostNeedAmount"] + (decimal)item["BankNeedAmount"];
                    item["Percent"] = (decimal)item["PostPercent"] + (decimal)item["BankPercent"];
                    item["NeedAmountPercent"] = (decimal)item["PostNeedAmountPercent"] + (decimal)item["BankNeedAmountPercent"];

                    report["DocCount"] = ((int?)report["DocCount"] ?? 0) + (int)item["DocCount"];
                    report["AppCount"] = ((int?)report["AppCount"] ?? 0) + (int)item["AppCount"];
                    report["NeedAmount"] = ((decimal?)report["NeedAmount"] ?? 0) + (decimal)item["NeedAmount"];
                    report["Percent"] = ((decimal?)report["Percent"] ?? 0) + (decimal)item["Percent"];
                    report["NeedAmountPercent"] = ((decimal?)report["NeedAmountPercent"] ?? 0) + (decimal)item["NeedAmountPercent"];
                    FieldsInit(item, _item);
                    i++;
                }
            }

            public class ReportItem
            {
                public int RowNo { get; set; }
                public string RowName { get; set; }
                public decimal Percent { get; set; }
                public string Category { get; set; }
                public int AppCount { get; set; }
                public decimal NeedAmountPercent { get; set; }
                public decimal PaymentPrice { get; set; }
                public decimal NeedAmount { get; set; }
                public int DocCount { get; set; }
                public int PostAppCount { get; set; }
                public decimal PostPercent { get; set; }
                public decimal PostNeedAmountPercent { get; set; }
                public decimal PostNeedAmount { get; set; }
                public int PostDocCount { get; set; }
                public int PostSection1DocCount { get; set; }
                public int PostSection1AppCount { get; set; }
                public decimal PostSection1Percent { get; set; }
                public decimal PostSection1NeedAmountPercent { get; set; }
                public int PostSection1MonthCount { get; set; }
                public decimal PostSection1NeedAmount { get; set; }
                public int PostSection2AppCount { get; set; }
                public int PostSection2MonthCount { get; set; }
                public decimal PostSection2NeedAmountPercent { get; set; }
                public decimal PostSection2Percent { get; set; }
                public int BankAppCount { get; set; }
                public int PostSection2DocCount { get; set; }
                public decimal PostSection2NeedAmount { get; set; }
                public int BankDocCount { get; set; }
                public decimal BankNeedAmountPercent { get; set; }
                public decimal BankPercent { get; set; }
                public decimal BankNeedAmount { get; set; }
                public int BankSection2DocCount { get; set; }
                public decimal BankSection2NeedAmount { get; set; }
                public decimal BankSection2NeedAmountPercent { get; set; }
                public int BankSection2MonthCount { get; set; }
                public decimal BankSection2Percent { get; set; }
                public int BankSection2AppCount { get; set; }
                public decimal BankSection1NeedAmount { get; set; }
                public int BankSection1DocCount { get; set; }
                public decimal BankSection1Percent { get; set; }
                public decimal BankSection1NeedAmountPercent { get; set; }
                public int BankSection1AppCount { get; set; }
                public int BankSection1MonthCount { get; set; }
            }

            private static void FieldsInit(Doc item, ReportItem _item = null)
            {
                item["DocCount"] = (int?)item["DocCount"] ?? 0;
                item["AppCount"] = (int?)item["AppCount"] ?? 0;
                item["NeedAmount"] = (decimal?)item["NeedAmount"] ?? 0;
                item["Percent"] = (decimal?)item["Percent"] ?? 0;
                item["NeedAmountPercent"] = (decimal?)item["NeedAmountPercent"] ?? 0;
                item["PostSection1MonthCount"] = (int?)item["PostSection1MonthCount"] ?? 0;
                item["PostSection1AppCount"] = (int?)item["PostSection1AppCount"] ?? 0;
                item["PostSection1NeedAmount"] = (decimal?)item["PostSection1NeedAmount"] ?? 0;
                item["PostSection1Percent"] = (decimal?)item["PostSection1Percent"] ?? 0;
                item["PostSection2DocCount"] = (int?)item["PostSection2DocCount"] ?? 0;
                item["PostSection2AppCount"] = (int?)item["PostSection2AppCount"] ?? 0;
                item["PostSection2NeedAmount"] = (decimal?)item["PostSection2NeedAmount"] ?? 0;
                item["PostSection2Percent"] = (decimal?)item["PostSection2Percent"] ?? 0;
                item["PostAppCount"] = (int?)item["PostAppCount"] ?? 0;
                item["PostNeedAmount"] = (decimal?)item["PostNeedAmount"] ?? 0;
                item["PostPercent"] = (decimal?)item["PostPercent"] ?? 0;
                item["PostNeedAmountPercent"] = (decimal?)item["PostNeedAmountPercent"] ?? 0;

                item["BankSection1MonthCount"] = (int?)item["BankSection1MonthCount"] ?? 0;
                item["BankSection1AppCount"] = (int?)item["BankSection1AppCount"] ?? 0;
                item["BankSection1NeedAmount"] = (decimal?)item["BankSection1NeedAmount"] ?? 0;
                item["BankSection1Percent"] = (decimal?)item["BankSection1Percent"] ?? 0;
                item["BankSection2DocCount"] = (int?)item["BankSection2DocCount"] ?? 0;
                item["BankSection2AppCount"] = (int?)item["BankSection2AppCount"] ?? 0;
                item["BankSection2NeedAmount"] = (decimal?)item["BankSection2NeedAmount"] ?? 0;
                item["BankSection2Percent"] = (decimal?)item["BankSection2Percent"] ?? 0;
                item["BankAppCount"] = (int?)item["BankAppCount"] ?? 0;
                item["BankNeedAmount"] = (decimal?)item["BankNeedAmount"] ?? 0;
                item["BankPercent"] = (decimal?)item["BankPercent"] ?? 0;
                item["BankNeedAmountPercent"] = (decimal?)item["BankNeedAmountPercent"] ?? 0;
                if (_item != null)
                {
                    _item.DocCount = (int?)item["DocCount"] ?? 0;
                    _item.AppCount = (int?)item["AppCount"] ?? 0;
                    _item.NeedAmount = (decimal?)item["NeedAmount"] ?? 0;
                    _item.Percent = (decimal?)item["Percent"] ?? 0;
                    _item.NeedAmountPercent = (decimal?)item["NeedAmountPercent"] ?? 0;
                    _item.PostSection1MonthCount = (int?)item["PostSection1MonthCount"] ?? 0;
                    _item.PostSection1AppCount = (int?)item["PostSection1AppCount"] ?? 0;
                    _item.PostSection1NeedAmount = (decimal?)item["PostSection1NeedAmount"] ?? 0;
                    _item.PostSection1Percent = (decimal?)item["PostSection1Percent"] ?? 0;
                    _item.PostSection2DocCount = (int?)item["PostSection2DocCount"] ?? 0;
                    _item.PostSection2AppCount = (int?)item["PostSection2AppCount"] ?? 0;
                    _item.PostSection2NeedAmount = (decimal?)item["PostSection2NeedAmount"] ?? 0;
                    _item.PostSection2Percent = (decimal?)item["PostSection2Percent"] ?? 0;
                    _item.PostAppCount = (int?)item["PostAppCount"] ?? 0;
                    _item.PostNeedAmount = (decimal?)item["PostNeedAmount"] ?? 0;
                    _item.PostPercent = (decimal?)item["PostPercent"] ?? 0;
                    _item.PostNeedAmountPercent = (decimal?)item["PostNeedAmountPercent"] ?? 0;

                    _item.BankSection1MonthCount = (int?)item["BankSection1MonthCount"] ?? 0;
                    _item.BankSection1AppCount = (int?)item["BankSection1AppCount"] ?? 0;
                    _item.BankSection1NeedAmount = (decimal?)item["BankSection1NeedAmount"] ?? 0;
                    _item.BankSection1Percent = (decimal?)item["BankSection1Percent"] ?? 0;
                    _item.BankSection2DocCount = (int?)item["BankSection2DocCount"] ?? 0;
                    _item.BankSection2AppCount = (int?)item["BankSection2AppCount"] ?? 0;
                    _item.BankSection2NeedAmount = (decimal?)item["BankSection2NeedAmount"] ?? 0;
                    _item.BankSection2Percent = (decimal?)item["BankSection2Percent"] ?? 0;
                    _item.BankAppCount = (int?)item["BankAppCount"] ?? 0;
                    _item.BankNeedAmount = (decimal?)item["BankNeedAmount"] ?? 0;
                    _item.BankPercent = (decimal?)item["BankPercent"] ?? 0;
                    _item.BankNeedAmountPercent = (decimal?)item["BankNeedAmountPercent"] ?? 0;
                }
            }

            private static void CalcAppCount(Guid userId, Guid orderDefId, int year, int month, Guid orgId, List<Doc> items, string orderTypeName, decimal postPercent, WorkflowContext context, Guid sectionTypeId)
            {
                SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                var orderSrc = query.JoinSource(query.Source, orderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                query.AddCondition(ExpressionOperation.And, orderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                query.AddCondition(ExpressionOperation.And, orderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, despAksyPaymentTypeId);
                query.AddAttribute(appSrc, "Assignments", SqlQuerySummaryFunction.Count);
                var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                query.AddGroupAttribute(appPaymentAttr);
                var table = new DataTable();
                using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }

                foreach (DataRow row in table.Rows)
                {
                    int count = row[0] is DBNull ? 0 : (int)row[0];
                    Guid categoryId;
                    Guid.TryParse(row[1].ToString(), out categoryId);
                    if (categoryId == Guid.Empty)
                        throw new ApplicationException("Ошибка при чтении кол-ва чел. - " + orderTypeName + ". Категория не найдена.");
                    var item = items[categoryTypes.IndexOf(categoryId)];
                    var sNum = sectionTypeId == section1TypeId ? 1 : 2;
                    var field = orderTypeName + "Section" + sNum + "AppCount";
                    item[field] = ((int?)item[field] ?? 0) + count;
                }
                if (orderDefId != bankOrderDefId) CalcAppCount(userId, bankOrderDefId, year, month, orgId, items, "Bank", postPercent, context, sectionTypeId);
            }

            private static void CalcOrdersCount(Guid userId, Guid orderDefId, int year, int month, Guid orgId, List<Doc> items, string orderTypeName, WorkflowContext context)
            {
                SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                var orderSrc = query.JoinSource(query.Source, orderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, section2TypeId);
                query.AddCondition(ExpressionOperation.And, orderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                query.AddCondition(ExpressionOperation.And, orderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, despAksyPaymentTypeId);
                query.AddAttribute(orderSrc, "&Id", SqlQuerySummaryFunction.Count);
                var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                query.AddGroupAttribute(appPaymentAttr);
                var table = new DataTable();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }
                foreach (DataRow row in table.Rows)
                {
                    int docCount = row[0] is DBNull ? 0 : (int)row[0];
                    Guid categoryId;
                    Guid.TryParse(row[1].ToString(), out categoryId);
                    if (categoryId == Guid.Empty) continue;
                    Doc item = items[categoryTypes.IndexOf(categoryId)];
                    item[orderTypeName + "Section2DocCount"] = docCount;
                }
                if (orderDefId != bankOrderDefId)
                    CalcOrdersCount(userId, bankOrderDefId, year, month, orgId, items, "Bank", context);
            }

            private static void CalcAmountSum(Guid userId, int year, int month, Guid orgId, List<Doc> items, decimal postPercent, string orderTypeName, WorkflowContext context, Guid sectionTypeId)
            {
                var field = orderTypeName + "Section" + (sectionTypeId == section1TypeId ? 1 : 2);
                if (orderTypeName == "Post")
                {
                    SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                    var orderSrc = query.JoinSource(query.Source, postOrderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                    var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                    query.AddCondition(ExpressionOperation.And, postOrderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                    query.AddCondition(ExpressionOperation.And, postOrderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                    query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, despAksyPaymentTypeId);
                    query.AddAttribute("Amount", SqlQuerySummaryFunction.Sum);
                    var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                    query.AddGroupAttribute(appPaymentAttr);
                    var table = new DataTable();
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }

                    foreach (DataRow row in table.Rows)
                    {
                        decimal amount = row[0] is DBNull ? 0 : (decimal)row[0];
                        Guid categoryId;
                        Guid.TryParse(row[1].ToString(), out categoryId);
                        if (categoryId == Guid.Empty) continue;
                        decimal service = (amount / 100) * postPercent;

                        var item = items[categoryTypes.IndexOf(categoryId)];
                        item[field + "NeedAmount"] = ((decimal?)item[field + "NeedAmount"] ?? 0m) + amount;
                        item[field + "Percent"] = ((decimal?)item[field + "Percent"] ?? 0m) + service;
                    }
                    CalcAmountSum(userId, year, month, orgId, items, postPercent, "Bank", context, sectionTypeId);
                }
                else
                {
                    SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                    var orderSrc = query.JoinSource(query.Source, bankOrderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                    var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                    var bankSrc = query.JoinSource(orderSrc, bankDefId, SqlSourceJoinType.Inner, "Bank");
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                    query.AddCondition(ExpressionOperation.And, bankOrderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                    query.AddCondition(ExpressionOperation.And, bankOrderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                    query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, despAksyPaymentTypeId);
                    query.AddAttribute("Amount", SqlQuerySummaryFunction.Sum);
                    var bankAttr = query.AddAttribute(bankSrc, "Percent");
                    var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                    query.AddGroupAttribute(appPaymentAttr);
                    query.AddGroupAttribute(bankAttr);
                    var table = new DataTable();
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }

                    foreach (DataRow row in table.Rows)
                    {
                        decimal amount = row[0] is DBNull ? 0 : (decimal)row[0];
                        double percent;
                        double.TryParse(row[1].ToString(), out percent);
                        Guid categoryId;
                        Guid.TryParse(row[2].ToString(), out categoryId);
                        if (categoryId == Guid.Empty) continue;
                        decimal service = (amount / 100) * (decimal)percent;

                        var item = items[categoryTypes.IndexOf(categoryId)];
                        item[field + "NeedAmount"] = ((decimal?)item[field + "NeedAmount"] ?? 0m) + amount;
                        item[field + "Percent"] = ((decimal?)item[field + "Percent"] ?? 0m) + service;
                    }
                }
            }

            private static Dictionary<Guid, Decimal> GetPriceList(Guid userId, int month, int year, WorkflowContext context)
            {
                Dictionary<Guid, Decimal> catList = new Dictionary<Guid, Decimal>();
                QueryBuilder qb = new QueryBuilder(tariffDefId, userId);
                qb.Where("EffectiveDate").Le(new DateTime(year, month, 1)).And("ExpiryDate").Ge(new DateTime(year, month,
                DateTime.DaysInMonth(year, month)))
                .And("PaymentType").Eq(despAksyPaymentTypeId);
                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("Category");
                query.AddAttribute("Amount");
                using (DataTable table = new DataTable())
                {
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    foreach (DataRow row in table.Rows)
                    {
                        Guid categoryId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                        if (categoryId != Guid.Empty)
                            if (!catList.ContainsKey(categoryId))
                                catList.Add(categoryId, row[1] is DBNull ? 0 : (decimal)row[1]);
                            else
                                throw new ApplicationException("Ошибка при чтении тарифов ДЕСП. Сообщите администрации об этой ошибке.");
                    }
                }
                return catList;
            }

            public static decimal GetPostPercent(int year, int month, Guid userId, WorkflowContext context)
            {
                SqlQuery query = new SqlQuery(context.DataContext, postDefId, userId);
                query.AddCondition(ExpressionOperation.And, postDefId, "DateFrom", ConditionOperation.LessEqual, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, postDefId, "DateTo", ConditionOperation.GreatEqual, new DateTime(year, month, DateTime.DaysInMonth(year, month)));
                query.AddAttribute("Size");
                using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    if (reader.Read()) return !reader.Reader.IsDBNull(0) ? reader.GetDecimal(0) : 0;
                return 0m;
            }
        }

        public static class AdditionalSocialBenefitsReport1
        {
            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {

                try
                {
                    var ui = context.GetUserInfo();
                    return Build(year, month, context.UserId, (Guid)ui.OrganizationId, context);
                }
                catch (Exception ex)
                {
                    throw new ApplicationException(ex.TargetSite.Name + "; " + ex.Message);
                }
            }
            private static readonly Guid despPaymentTypeId = new Guid("{272F8D91-5B4B-42AC-9D79-A17E77F5496E}");
            // Document Defs                                                    
            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{9580E9AE-5949-4B83-90F0-EED511B63477}");
            private static readonly Guid reportItemDefId = new Guid("{D2FCA75E-34E4-4E85-93B5-6917C4F18BC2}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            // States
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid canceledStateId = new Guid("{C65E63CC-54E9-424E-AFDC-5A8DB1CB04A3}"); // Аннулирован
            private static readonly Guid onApprovingStateId = new Guid("{5CD9E88D-671E-4A44-AD92-9F74DA3B47F7}"); //На утверждении
            private static readonly Guid onRegisteringStateId = new Guid("{A9CD37C4-A718-4DE1-9E95-EC8EC280C8D4}"); // На регистрации
            private static readonly Guid refusedStateId = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}"); // Отказан

            private static readonly Guid categoryType1Id = new Guid("{608F740E-C44B-4305-88E8-F53D497214C0}");//Дети погибшего в событиях 2010 г. до 18 лет
            private static readonly Guid categoryType2Id = new Guid("{238BD896-A3B7-494D-B549-4AAC292EE8F5}");//Родители погибшего ед. ребенка в событиях 2010 г. по достижении пенсионного возраста
            private static readonly Guid categoryType3Id = new Guid("{ECDF4DE6-299E-4C6A-843F-49362C32EB96}");//Получившие вред здоровью в событиях 2010 г., имеющие заключение СМЭ
            private static readonly Guid categoryType4Id = new Guid("{7D08F570-239D-46BF-BF4F-6E3A510ACCD2}");//Признанные ЛОВЗ в следствие событий 2010 г.
            private static readonly List<object> categoryTypes = new List<object>
            {
            categoryType1Id, categoryType2Id, categoryType3Id, categoryType4Id
            };

            private static Doc GetReportDoc(WorkflowContext context, List<object[]> reports)
            {
                Doc report;
                var docRepo = context.Documents;
                var approvedReportId = reports.FirstOrDefault(r => (Guid)r[1] == approvedStateId);
                if (approvedReportId != null)
                {
                    context["ApprovedReport"] = docRepo.LoadById((Guid)approvedReportId[0]);
                    return null;
                }
                reports = reports.Where(r => (Guid)r[1] == onRegisteringStateId).ToList();
                if (reports.Count > 0)
                {
                    var onRegisteringReportId = reports.FirstOrDefault(x => (DateTime)x[2] == reports.Max(r => (DateTime)r[2]));
                    report = docRepo.LoadById((Guid)onRegisteringReportId[0]);
                    reports.RemoveAll(x => (Guid)x[0] == (Guid)onRegisteringReportId[0]);
                    report["DocCount"] = null;
                    report["AppCount"] = null;
                    report["NeedAmount"] = null;
                    report["Percent"] = null;
                    report["NeedAmountPercent"] = null;
                    docRepo.ClearAttrDocList(report.Id, report.Get<DocListAttribute>("Rows").AttrDef.Id);
                    foreach (var reportId in reports.Select(x => (Guid)x[0]))
                    {
                        docRepo.SetDocState(reportId, refusedStateId);
                    }
                }
                else
                    report = docRepo.New(reportDefId);
                return report;
            }
            public static List<ReportItem> Build(int year, int month, Guid userId, Guid orgId, WorkflowContext context)
            {
                if (year < 2011 || year > 3000)
                    throw new ApplicationException("Ошибка в значении года!");
                if (month < 1 || month > 12)
                    throw new ApplicationException("Ошибка в значении месяца!");
                var docRepo = context.Documents;
                var qb = new QueryBuilder(reportDefId);
                qb.Where("&OrgId").Eq(orgId).And("Month").Eq(month).And("Year").Eq(year).And("&State").Neq(refusedStateId);

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("&Id");
                query.AddAttribute("&State");
                query.AddAttribute("&Created");
                var reports = new List<object[]>();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        reports.Add(new object[] { reader.GetGuid(0), reader.GetGuid(1), reader.GetDateTime(2) });
                    }
                }

                Dictionary<Guid, Decimal> prices = GetPriceList(userId, month, year, context);

                decimal postPercent = GetPostPercent(year, month, userId, context);
                List<Doc> items = new List<Doc>();
                var reportItemList = new List<ReportItem>();

                //Инициализация строк отчета
                foreach (Guid categoryId in categoryTypes)
                {
                    Doc item = docRepo.New(reportItemDefId);
                    item["Category"] = categoryId;
                    item["PaymentPrice"] = prices[categoryId];
                    reportItemList.Add(new ReportItem { Category = context.Enums.GetValue(categoryId).Value, PaymentPrice = prices[categoryId] });
                    items.Add(item);
                }

                Doc report = GetReportDoc(context, reports);
                if (report == null)
                {
                    return InitReport(context, context.Get<Doc>("ApprovedReport"), reportItemList);
                }


                CalcSection1(items, context, year, month, orgId, userId, postPercent);
                CalcSection2(items, context, year, month, orgId, userId, postPercent);
                CalcFields(items, report, reportItemList);

                items.Reverse();
                foreach (Doc item in items)
                {
                    docRepo.Save(item);
                    docRepo.AddDocToList(item.Id, report, "Rows");
                }
                report["Year"] = year;
                report["Month"] = month;
                report["Organization"] = orgId;

                docRepo.Save(report);
                docRepo.SetDocState(report, onRegisteringStateId);
                return reportItemList;
            }

            static List<ReportItem> InitReport(WorkflowContext context, Doc report, List<ReportItem> reportItemList)
            {
                var reportItems = new List<Doc>();
                var docRepo = context.Documents;
                foreach (var rItemId in docRepo.DocAttrList(out int c, report, "Rows", 0, 0))
                {
                    var curDocRepo = docRepo.LoadById(rItemId);
                    if (categoryTypes.Contains((Guid)curDocRepo["Category"]))
                        reportItems.Add(curDocRepo);
                }
                CalcFields(reportItems, report, reportItemList);
                return reportItemList;
            }

            private static void CalcSection1(List<Doc> items, WorkflowContext context, int year, int month, Guid orgId, Guid userId, decimal postPercent)
            {
                CalcAmountSum(userId, year, month, orgId, items, postPercent, "Post", context, section1TypeId);
            }

            private static void CalcSection2(List<Doc> items, WorkflowContext context, int year, int month, Guid orgId, Guid userId, decimal postPercent)
            {
                CalcAppCount(userId, postOrderDefId, year, month, orgId, items, "Post", postPercent, context, section2TypeId);
                CalcOrdersCount(userId, postOrderDefId, year, month, orgId, items, "Post", context);
                CalcAmountSum(userId, year, month, orgId, items, postPercent, "Post", context, section2TypeId);
            }

            private static void CalcFields(List<Doc> items, Doc report, List<ReportItem> _items)
            {
                int i = 0;
                foreach (var item in items)
                {
                    var _item = _items[i];
                    FieldsInit(item);
                    item["PostNeedAmount"] = (decimal)item["PostSection1NeedAmount"] + (decimal)item["PostSection2NeedAmount"];
                    item["PostPercent"] = (decimal)item["PostSection1Percent"] + (decimal)item["PostSection2Percent"];
                    item["PostNeedAmountPercent"] = (decimal)item["PostNeedAmount"] + (decimal)item["PostPercent"];

                    item["BankNeedAmount"] = (decimal)item["BankSection1NeedAmount"] + (decimal)item["BankSection2NeedAmount"];
                    item["BankPercent"] = (decimal)item["BankSection1Percent"] + (decimal)item["BankSection2Percent"];
                    item["BankNeedAmountPercent"] = (decimal)item["BankNeedAmount"] + (decimal)item["BankPercent"];

                    item["DocCount"] = (int)item["PostSection2DocCount"] + (int)item["BankSection2DocCount"];
                    item["AppCount"] = (int)item["PostSection2AppCount"] + (int)item["BankSection2AppCount"];
                    item["NeedAmount"] = (decimal)item["PostNeedAmount"] + (decimal)item["BankNeedAmount"];
                    item["Percent"] = (decimal)item["PostPercent"] + (decimal)item["BankPercent"];
                    item["NeedAmountPercent"] = (decimal)item["PostNeedAmountPercent"] + (decimal)item["BankNeedAmountPercent"];

                    report["DocCount"] = ((int?)report["DocCount"] ?? 0) + (int)item["DocCount"];
                    report["AppCount"] = ((int?)report["AppCount"] ?? 0) + (int)item["AppCount"];
                    report["NeedAmount"] = ((decimal?)report["NeedAmount"] ?? 0) + (decimal)item["NeedAmount"];
                    report["Percent"] = ((decimal?)report["Percent"] ?? 0) + (decimal)item["Percent"];
                    report["NeedAmountPercent"] = ((decimal?)report["NeedAmountPercent"] ?? 0) + (decimal)item["NeedAmountPercent"];
                    FieldsInit(item, _item);
                    i++;
                }
            }

            public class ReportItem
            {
                public int RowNo { get; set; }
                public string RowName { get; set; }
                public decimal Percent { get; set; }
                public string Category { get; set; }
                public int AppCount { get; set; }
                public decimal NeedAmountPercent { get; set; }
                public decimal PaymentPrice { get; set; }
                public decimal NeedAmount { get; set; }
                public int DocCount { get; set; }
                public int PostAppCount { get; set; }
                public decimal PostPercent { get; set; }
                public decimal PostNeedAmountPercent { get; set; }
                public decimal PostNeedAmount { get; set; }
                public int PostDocCount { get; set; }
                public int PostSection1DocCount { get; set; }
                public int PostSection1AppCount { get; set; }
                public decimal PostSection1Percent { get; set; }
                public decimal PostSection1NeedAmountPercent { get; set; }
                public int PostSection1MonthCount { get; set; }
                public decimal PostSection1NeedAmount { get; set; }
                public int PostSection2AppCount { get; set; }
                public int PostSection2MonthCount { get; set; }
                public decimal PostSection2NeedAmountPercent { get; set; }
                public decimal PostSection2Percent { get; set; }
                public int BankAppCount { get; set; }
                public int PostSection2DocCount { get; set; }
                public decimal PostSection2NeedAmount { get; set; }
                public int BankDocCount { get; set; }
                public decimal BankNeedAmountPercent { get; set; }
                public decimal BankPercent { get; set; }
                public decimal BankNeedAmount { get; set; }
                public int BankSection2DocCount { get; set; }
                public decimal BankSection2NeedAmount { get; set; }
                public decimal BankSection2NeedAmountPercent { get; set; }
                public int BankSection2MonthCount { get; set; }
                public decimal BankSection2Percent { get; set; }
                public int BankSection2AppCount { get; set; }
                public decimal BankSection1NeedAmount { get; set; }
                public int BankSection1DocCount { get; set; }
                public decimal BankSection1Percent { get; set; }
                public decimal BankSection1NeedAmountPercent { get; set; }
                public int BankSection1AppCount { get; set; }
                public int BankSection1MonthCount { get; set; }
            }

            private static void FieldsInit(Doc item, ReportItem _item = null)
            {
                item["DocCount"] = (int?)item["DocCount"] ?? 0;
                item["AppCount"] = (int?)item["AppCount"] ?? 0;
                item["NeedAmount"] = (decimal?)item["NeedAmount"] ?? 0;
                item["Percent"] = (decimal?)item["Percent"] ?? 0;
                item["NeedAmountPercent"] = (decimal?)item["NeedAmountPercent"] ?? 0;
                item["PostSection1MonthCount"] = (int?)item["PostSection1MonthCount"] ?? 0;
                item["PostSection1AppCount"] = (int?)item["PostSection1AppCount"] ?? 0;
                item["PostSection1NeedAmount"] = (decimal?)item["PostSection1NeedAmount"] ?? 0;
                item["PostSection1Percent"] = (decimal?)item["PostSection1Percent"] ?? 0;
                item["PostSection2DocCount"] = (int?)item["PostSection2DocCount"] ?? 0;
                item["PostSection2AppCount"] = (int?)item["PostSection2AppCount"] ?? 0;
                item["PostSection2NeedAmount"] = (decimal?)item["PostSection2NeedAmount"] ?? 0;
                item["PostSection2Percent"] = (decimal?)item["PostSection2Percent"] ?? 0;
                item["PostAppCount"] = (int?)item["PostAppCount"] ?? 0;
                item["PostNeedAmount"] = (decimal?)item["PostNeedAmount"] ?? 0;
                item["PostPercent"] = (decimal?)item["PostPercent"] ?? 0;
                item["PostNeedAmountPercent"] = (decimal?)item["PostNeedAmountPercent"] ?? 0;

                item["BankSection1MonthCount"] = (int?)item["BankSection1MonthCount"] ?? 0;
                item["BankSection1AppCount"] = (int?)item["BankSection1AppCount"] ?? 0;
                item["BankSection1NeedAmount"] = (decimal?)item["BankSection1NeedAmount"] ?? 0;
                item["BankSection1Percent"] = (decimal?)item["BankSection1Percent"] ?? 0;
                item["BankSection2DocCount"] = (int?)item["BankSection2DocCount"] ?? 0;
                item["BankSection2AppCount"] = (int?)item["BankSection2AppCount"] ?? 0;
                item["BankSection2NeedAmount"] = (decimal?)item["BankSection2NeedAmount"] ?? 0;
                item["BankSection2Percent"] = (decimal?)item["BankSection2Percent"] ?? 0;
                item["BankAppCount"] = (int?)item["BankAppCount"] ?? 0;
                item["BankNeedAmount"] = (decimal?)item["BankNeedAmount"] ?? 0;
                item["BankPercent"] = (decimal?)item["BankPercent"] ?? 0;
                item["BankNeedAmountPercent"] = (decimal?)item["BankNeedAmountPercent"] ?? 0;
                if (_item != null)
                {
                    _item.DocCount = (int?)item["DocCount"] ?? 0;
                    _item.AppCount = (int?)item["AppCount"] ?? 0;
                    _item.NeedAmount = (decimal?)item["NeedAmount"] ?? 0;
                    _item.Percent = (decimal?)item["Percent"] ?? 0;
                    _item.NeedAmountPercent = (decimal?)item["NeedAmountPercent"] ?? 0;
                    _item.PostSection1MonthCount = (int?)item["PostSection1MonthCount"] ?? 0;
                    _item.PostSection1AppCount = (int?)item["PostSection1AppCount"] ?? 0;
                    _item.PostSection1NeedAmount = (decimal?)item["PostSection1NeedAmount"] ?? 0;
                    _item.PostSection1Percent = (decimal?)item["PostSection1Percent"] ?? 0;
                    _item.PostSection2DocCount = (int?)item["PostSection2DocCount"] ?? 0;
                    _item.PostSection2AppCount = (int?)item["PostSection2AppCount"] ?? 0;
                    _item.PostSection2NeedAmount = (decimal?)item["PostSection2NeedAmount"] ?? 0;
                    _item.PostSection2Percent = (decimal?)item["PostSection2Percent"] ?? 0;
                    _item.PostAppCount = (int?)item["PostAppCount"] ?? 0;
                    _item.PostNeedAmount = (decimal?)item["PostNeedAmount"] ?? 0;
                    _item.PostPercent = (decimal?)item["PostPercent"] ?? 0;
                    _item.PostNeedAmountPercent = (decimal?)item["PostNeedAmountPercent"] ?? 0;

                    _item.BankSection1MonthCount = (int?)item["BankSection1MonthCount"] ?? 0;
                    _item.BankSection1AppCount = (int?)item["BankSection1AppCount"] ?? 0;
                    _item.BankSection1NeedAmount = (decimal?)item["BankSection1NeedAmount"] ?? 0;
                    _item.BankSection1Percent = (decimal?)item["BankSection1Percent"] ?? 0;
                    _item.BankSection2DocCount = (int?)item["BankSection2DocCount"] ?? 0;
                    _item.BankSection2AppCount = (int?)item["BankSection2AppCount"] ?? 0;
                    _item.BankSection2NeedAmount = (decimal?)item["BankSection2NeedAmount"] ?? 0;
                    _item.BankSection2Percent = (decimal?)item["BankSection2Percent"] ?? 0;
                    _item.BankAppCount = (int?)item["BankAppCount"] ?? 0;
                    _item.BankNeedAmount = (decimal?)item["BankNeedAmount"] ?? 0;
                    _item.BankPercent = (decimal?)item["BankPercent"] ?? 0;
                    _item.BankNeedAmountPercent = (decimal?)item["BankNeedAmountPercent"] ?? 0;
                }
            }

            private static void CalcAppCount(Guid userId, Guid orderDefId, int year, int month, Guid orgId, List<Doc> items, string orderTypeName, decimal postPercent, WorkflowContext context, Guid sectionTypeId)
            {
                SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                var orderSrc = query.JoinSource(query.Source, orderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                query.AddCondition(ExpressionOperation.And, orderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                query.AddCondition(ExpressionOperation.And, orderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, despPaymentTypeId);
                query.AddAttribute(appSrc, "Assignments", SqlQuerySummaryFunction.Count);
                var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                query.AddGroupAttribute(appPaymentAttr);
                var table = new DataTable();
                using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }
                foreach (DataRow row in table.Rows)
                {
                    int count = row[0] is DBNull ? 0 : (int)row[0];
                    Guid categoryId;
                    Guid.TryParse(row[1].ToString(), out categoryId);
                    if (categoryId == Guid.Empty)
                        throw new ApplicationException("Ошибка при чтении кол-ва чел. - " + orderTypeName + ". Категория не найдена.");
                    var item = items[categoryTypes.IndexOf(categoryId)];
                    var sNum = sectionTypeId == section1TypeId ? 1 : 2;
                    var field = orderTypeName + "Section" + sNum + "AppCount";
                    item[field] = ((int?)item[field] ?? 0) + count;
                }
                if (orderDefId != bankOrderDefId) CalcAppCount(userId, bankOrderDefId, year, month, orgId, items, "Bank", postPercent, context, sectionTypeId);
            }

            private static void CalcOrdersCount(Guid userId, Guid orderDefId, int year, int month, Guid orgId, List<Doc> items, string orderTypeName, WorkflowContext context)
            {
                SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                var orderSrc = query.JoinSource(query.Source, orderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, section2TypeId);
                query.AddCondition(ExpressionOperation.And, orderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                query.AddCondition(ExpressionOperation.And, orderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, despPaymentTypeId);
                query.AddAttribute(orderSrc, "&Id", SqlQuerySummaryFunction.Count);
                var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                query.AddGroupAttribute(appPaymentAttr);
                var table = new DataTable();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }
                foreach (DataRow row in table.Rows)
                {
                    int docCount = row[0] is DBNull ? 0 : (int)row[0];
                    Guid categoryId;
                    Guid.TryParse(row[1].ToString(), out categoryId);
                    if (categoryId == Guid.Empty) continue;
                    Doc item = items[categoryTypes.IndexOf(categoryId)];
                    item[orderTypeName + "Section2DocCount"] = docCount;
                }
                if (orderDefId != bankOrderDefId)
                    CalcOrdersCount(userId, bankOrderDefId, year, month, orgId, items, "Bank", context);
            }

            private static void CalcAmountSum(Guid userId, int year, int month, Guid orgId, List<Doc> items, decimal postPercent, string orderTypeName, WorkflowContext context, Guid sectionTypeId)
            {
                var field = orderTypeName + "Section" + (sectionTypeId == section1TypeId ? 1 : 2);
                if (orderTypeName == "Post")
                {
                    SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                    var orderSrc = query.JoinSource(query.Source, postOrderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                    var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                    query.AddCondition(ExpressionOperation.And, postOrderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                        canceledStateId, onApprovingStateId
                    });
                    query.AddCondition(ExpressionOperation.And, postOrderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                    query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, despPaymentTypeId);
                    query.AddAttribute("Amount", SqlQuerySummaryFunction.Sum);
                    var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                    query.AddGroupAttribute(appPaymentAttr);
                    var table = new DataTable();
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }

                    foreach (DataRow row in table.Rows)
                    {
                        decimal amount = row[0] is DBNull ? 0 : (decimal)row[0];
                        Guid categoryId;
                        Guid.TryParse(row[1].ToString(), out categoryId);
                        if (categoryId == Guid.Empty) continue;
                        decimal service = (amount / 100) * postPercent;

                        var item = items[categoryTypes.IndexOf(categoryId)];
                        item[field + "NeedAmount"] = ((decimal?)item[field + "NeedAmount"] ?? 0m) + amount;
                        item[field + "Percent"] = ((decimal?)item[field + "Percent"] ?? 0m) + service;
                    }
                    CalcAmountSum(userId, year, month, orgId, items, postPercent, "Bank", context, sectionTypeId);
                }
                else
                {
                    SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                    var orderSrc = query.JoinSource(query.Source, bankOrderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                    var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                    var bankSrc = query.JoinSource(orderSrc, bankDefId, SqlSourceJoinType.Inner, "Bank");
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                    query.AddCondition(ExpressionOperation.And, bankOrderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                        canceledStateId, onApprovingStateId
                    });
                    query.AddCondition(ExpressionOperation.And, bankOrderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                    query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, despPaymentTypeId);
                    query.AddAttribute("Amount", SqlQuerySummaryFunction.Sum);
                    var bankAttr = query.AddAttribute(bankSrc, "Percent");
                    var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                    query.AddGroupAttribute(appPaymentAttr);
                    query.AddGroupAttribute(bankAttr);
                    var table = new DataTable();
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }

                    foreach (DataRow row in table.Rows)
                    {
                        decimal amount = row[0] is DBNull ? 0 : (decimal)row[0];
                        double percent;
                        double.TryParse(row[1].ToString(), out percent);
                        Guid categoryId;
                        Guid.TryParse(row[2].ToString(), out categoryId);
                        if (categoryId == Guid.Empty) continue;
                        decimal service = (amount / 100) * (decimal)percent;

                        var item = items[categoryTypes.IndexOf(categoryId)];
                        item[field + "NeedAmount"] = ((decimal?)item[field + "NeedAmount"] ?? 0m) + amount;
                        item[field + "Percent"] = ((decimal?)item[field + "Percent"] ?? 0m) + service;
                    }
                }
            }

            private static Dictionary<Guid, Decimal> GetPriceList(Guid userId, int month, int year, WorkflowContext context)
            {
                Dictionary<Guid, Decimal> catList = new Dictionary<Guid, Decimal>();
                QueryBuilder qb = new QueryBuilder(tariffDefId, userId);
                qb.Where("EffectiveDate").Le(new DateTime(year, month, 1)).And("ExpiryDate").Ge(new DateTime(year, month,
                DateTime.DaysInMonth(year, month)))
                .And("PaymentType").Eq(despPaymentTypeId);
                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("Category");
                query.AddAttribute("Amount");
                using (DataTable table = new DataTable())
                {
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    foreach (DataRow row in table.Rows)
                    {
                        Guid categoryId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                        if (categoryId != Guid.Empty)
                            if (!catList.ContainsKey(categoryId))
                                catList.Add(categoryId, row[1] is DBNull ? 0 : (decimal)row[1]);
                            else
                                throw new ApplicationException("Ошибка при чтении тарифов ДЕСП. Сообщите администрации об этой ошибке.");
                    }
                }
                return catList;
            }

            public static decimal GetPostPercent(int year, int month, Guid userId, WorkflowContext context)
            {
                SqlQuery query = new SqlQuery(context.DataContext, postDefId, userId);
                query.AddCondition(ExpressionOperation.And, postDefId, "DateFrom", ConditionOperation.LessEqual, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, postDefId, "DateTo", ConditionOperation.GreatEqual, new DateTime(year, month, DateTime.DaysInMonth(year, month)));
                query.AddAttribute("Size");
                using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    if (reader.Read()) return !reader.Reader.IsDBNull(0) ? reader.GetDecimal(0) : 0;
                return 0m;
            }
        }

        public static class SocialBenefitsReport1
        {
            // Виды выплат
            private static readonly Guid retirementBenefitPaymentId = new Guid("{AFAA7F86-74AE-4260-9933-A56F7845E55A}");
            private static readonly Guid childBenefitPaymentId = new Guid("{AB3F8C41-897A-4574-BAA0-B7CD4AAA1C80}");
            private static readonly Guid invalidBenefitPaymentId = new Guid("{70C28E62-2387-4A59-917D-A366ADE119A8}");
            private static readonly Guid survivorBenefitPaymentId = new Guid("{839D5712-E75B-4E71-83F7-168CE4F089C0}");
            private static readonly Guid aidsFromBenefitPaymentId = new Guid("{3BEBE4F9-0B15-41CB-9B96-54E83819AB0F}");
            private static readonly Guid aidsBenefitPaymentId = new Guid("{47EEBFBC-A4E9-495D-A6A1-F87B5C3057C9}");
            private static readonly Guid orphanBenefitPaymentId = new Guid("{4F12C366-7E2F-4208-9CB8-4EAB6E6C0EF1}");

            private static readonly Guid alpineRetirementCategoryId = new Guid("{587D9992-DBB7-4BAD-A358-0FA571EBDB37}");
            private static readonly Guid retirementCategoryId = new Guid("{56DD1E0D-F693-470D-8756-6969DFA71A02}");
            private static readonly Guid heroMotherCategoryId = new Guid("{F21D59E6-BBAA-40D8-89FC-C7B3A707E8E6}");
            private static readonly Guid childDisableCategoryId = new Guid("{FDBDD774-EB88-46EA-9559-005D655BC196}");
            private static readonly Guid orphanChildren = new Guid("{401D9570-2A4E-453D-869E-9AA2603C9CD8}");
            private static readonly Guid childSurvivorsCategoryId = new Guid("{304A01B8-8A08-413E-B3D5-7B2C237829A2}");
            private static readonly Guid childFromAidsCategoryId = new Guid("{DE66F6D7-5462-4D45-8B20-6478728B5BD3}");
            private static readonly Guid childenAidsCategoryId = new Guid("{2222FD98-B885-4DC0-A0D4-271600AF281A}");
            private static readonly Guid childISPCategoryId = new Guid("{1E750C67-2DDF-488E-A4C4-D94547433067}");
            private static readonly Guid childhood1CategoryId = new Guid("{D18791C9-DE0A-4E15-92A8-20EF140C51ED}");
            private static readonly Guid childhood2CategoryId = new Guid("{305621EC-9ECC-4AF9-810D-5B639C339D50}");
            private static readonly Guid childhood3CategoryId = new Guid("{FD3B12FB-55D3-4229-975E-342AC126E942}");
            private static readonly Guid commonDeseas1CategoryId = new Guid("{409BCDA9-6770-4D3F-B515-7DE0E341C63D}");
            private static readonly Guid commonDeseas2CategoryId = new Guid("{0955ED04-8A32-476B-AE6E-E51DE0F2C66D}");
            private static readonly Guid commonDeseas3CategoryId = new Guid("{7B622DDA-D6C0-48CA-AFA9-7C74149D8BD5}");
            private static readonly Guid hearDisabled1CategoryId = new Guid("{C7B397DF-8325-4F0D-A94C-F8BD5E8152E4}");
            private static readonly Guid hearDisabled2CategoryId = new Guid("{18A54BA7-0330-40B3-AE9E-67D049004AB0}");
            private static readonly Guid hearDisabled3CategoryId = new Guid("{DB4E8C78-A17F-44FD-890D-A67F1EA2B457}");
            private static readonly Guid eyesDisabled1CategoryId = new Guid("{5EE97C99-B2A1-4759-9A28-876B08FE7BA8}");
            private static readonly Guid eyesDisabled2CategoryId = new Guid("{947E64F0-18EC-4A6B-80C0-FCECFE7C67C2}");
            private static readonly Guid eyesDisabled3CategoryId = new Guid("{8B787FCA-62EE-4261-9094-1FFAC1BF4C02}");

            public static List<object> categoryTypes = new List<object>
            {
                heroMotherCategoryId,
                alpineRetirementCategoryId,
                retirementCategoryId,
                orphanChildren,
                childSurvivorsCategoryId,
                commonDeseas3CategoryId,
                commonDeseas2CategoryId,
                commonDeseas1CategoryId,
                childhood3CategoryId,
                childhood2CategoryId,
                childhood1CategoryId,
                childenAidsCategoryId,
                childFromAidsCategoryId,
                childISPCategoryId,
                childDisableCategoryId
            };
            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{9118D82A-2AB4-40F2-A3BC-0BB54D34F3CE}");
            private static readonly Guid reportItemDefId = new Guid("{54167C45-6382-460E-8D72-CDE7D7B43F5C}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");
            // States
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid canceledStateId = new Guid("{C65E63CC-54E9-424E-AFDC-5A8DB1CB04A3}"); // Аннулирован
            private static readonly Guid onApprovingStateId = new Guid("{5CD9E88D-671E-4A44-AD92-9F74DA3B47F7}"); //На утверждении
            private static readonly Guid onRegisteringStateId = new Guid("{A9CD37C4-A718-4DE1-9E95-EC8EC280C8D4}"); // На регистрации
            private static readonly Guid refusedStateId = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}"); // Отказан

            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {
                try
                {
                    var ui = context.GetUserInfo();
                    return Build(year, month, context.UserId, (Guid)ui.OrganizationId, context);
                }
                catch (Exception ex)
                {
                    throw new ApplicationException("TargetSite: " + ex.TargetSite.Name + "; Message: " + ex.Message);
                }
            }
            private static Doc GetReportDoc(WorkflowContext context, List<object[]> reports)
            {
                Doc report;
                var docRepo = context.Documents;
                var approvedReportId = reports.FirstOrDefault(r => (Guid)r[1] == approvedStateId);
                if (approvedReportId != null)
                {
                    context["ApprovedReport"] = docRepo.LoadById((Guid)approvedReportId[0]);
                    return null;
                }
                reports = reports.Where(r => (Guid)r[1] == onRegisteringStateId).ToList();
                if (reports.Count > 0)
                {
                    var onRegisteringReportId = reports.FirstOrDefault(x => (DateTime)x[2] == reports.Max(r => (DateTime)r[2]));
                    report = docRepo.LoadById((Guid)onRegisteringReportId[0]);
                    reports.RemoveAll(x => (Guid)x[0] == (Guid)onRegisteringReportId[0]);
                    report["DocCount"] = null;
                    report["AppCount"] = null;
                    report["NeedAmount"] = null;
                    report["Percent"] = null;
                    report["NeedAmountPercent"] = null;
                    docRepo.ClearAttrDocList(report.Id, report.Get<DocListAttribute>("Rows").AttrDef.Id);
                    foreach (var reportId in reports.Select(x => (Guid)x[0]))
                    {
                        docRepo.SetDocState(reportId, refusedStateId);
                    }
                }
                else
                    report = docRepo.New(reportDefId);
                return report;
            }
            public static List<ReportItem> Build(int year, int month, Guid userId, Guid orgId, WorkflowContext context)
            {
                if (year < 2011 || year > 3000)
                    throw new ApplicationException("Ошибка в значении года!");
                if (month < 1 || month > 12)
                    throw new ApplicationException("Ошибка в значении месяца!");
                var docRepo = context.Documents;
                var qb = new QueryBuilder(reportDefId);
                qb.Where("&OrgId").Eq(orgId).And("Month").Eq(month).And("Year").Eq(year).And("&State").Neq(refusedStateId);

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("&Id");
                query.AddAttribute("&State");
                query.AddAttribute("&Created");
                var reports = new List<object[]>();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        reports.Add(new object[] { reader.GetGuid(0), reader.GetGuid(1), reader.GetDateTime(2) });
                    }
                }

                var prices = GetPriceList(userId, month, year, context);

                decimal postPercent = GetPostPercent(year, month, userId, context);
                List<Doc> items = new List<Doc>();
                List<ReportItem> _items = new List<ReportItem>();

                //Инициализация строк отчета
                foreach (Guid categoryId in categoryTypes)
                {
                    var item = docRepo.New(reportItemDefId);
                    item["Category"] = categoryId;
                    item["PaymentPrice"] = prices[categoryId];
                    var _item = new ReportItem { RowName = context.Enums.GetValue(categoryId).Value }; //*
                    items.Add(item);
                    _items.Add(_item);
                    _items.Reverse();
                    items.Reverse();
                }

                Doc report = GetReportDoc(context, reports);
                if (report == null)
                {
                    return InitReport(context, context.Get<Doc>("ApprovedReport"), _items);
                }



                CalcSection1(items, context, year, month, orgId, userId, postPercent);

                CalcSection2(items, context, year, month, orgId, userId, postPercent);

                CalcFields(items, report, _items);

                foreach (Doc item in items)
                {
                    docRepo.Save(item);
                    docRepo.AddDocToList(item.Id, report, "Rows");
                }
                report["Year"] = year;
                report["Month"] = month;
                report["Organization"] = orgId;

                docRepo.Save(report);
                docRepo.SetDocState(report, onRegisteringStateId);
                return _items;
            }

            static List<ReportItem> InitReport(WorkflowContext context, Doc report, List<ReportItem> reportItemList)
            {
                var reportItems = new List<Doc>();
                var docRepo = context.Documents;
                foreach (var rItemId in docRepo.DocAttrList(out int c, report, "Rows", 0, 0))
                {
                    var curDocRepo = docRepo.LoadById(rItemId);
                    if (categoryTypes.Contains((Guid)curDocRepo["Category"]))
                        reportItems.Add(curDocRepo);
                }
                CalcFields(reportItems, report, reportItemList);
                return reportItemList;
            }

            private static void CalcSection1(List<Doc> items, WorkflowContext context, int year, int month, Guid orgId, Guid userId, decimal postPercent)
            {
                CalcAmountSum(userId, year, month, orgId, items, postPercent, "Post", context, section1TypeId);
            }

            private static void CalcSection2(List<Doc> items, WorkflowContext context, int year, int month, Guid orgId, Guid userId, decimal postPercent)
            {
                CalcAppCount(userId, postOrderDefId, year, month, orgId, items, "Post", postPercent, context, section2TypeId);
                CalcOrdersCount(userId, postOrderDefId, year, month, orgId, items, "Post", context);
                CalcAmountSum(userId, year, month, orgId, items, postPercent, "Post", context, section2TypeId);
            }

            private static void CalcFields(List<Doc> items, Doc report, List<ReportItem> _items)
            {
                foreach (var item in items)
                {
                    FieldsInit(item);
                    item["PostNeedAmount"] = (decimal)item["PostSection1NeedAmount"] + (decimal)item["PostSection2NeedAmount"];
                    item["PostPercent"] = (decimal)item["PostSection1Percent"] + (decimal)item["PostSection2Percent"];
                    item["PostNeedAmountPercent"] = (decimal)item["PostNeedAmount"] + (decimal)item["PostPercent"];

                    item["BankNeedAmount"] = (decimal)item["BankSection1NeedAmount"] + (decimal)item["BankSection2NeedAmount"];
                    item["BankPercent"] = (decimal)item["BankSection1Percent"] + (decimal)item["BankSection2Percent"];
                    item["BankNeedAmountPercent"] = (decimal)item["BankNeedAmount"] + (decimal)item["BankPercent"];

                    item["DocCount"] = (int)item["PostSection2DocCount"] + (int)item["BankSection2DocCount"];
                    item["AppCount"] = (int)item["PostSection2AppCount"] + (int)item["BankSection2AppCount"];
                    item["NeedAmount"] = (decimal)item["PostNeedAmount"] + (decimal)item["BankNeedAmount"];
                    item["Percent"] = (decimal)item["PostPercent"] + (decimal)item["BankPercent"];
                    item["NeedAmountPercent"] = (decimal)item["PostNeedAmountPercent"] + (decimal)item["BankNeedAmountPercent"];

                    report["DocCount"] = ((int?)report["DocCount"] ?? 0) + (int)item["DocCount"];
                    report["AppCount"] = ((int?)report["AppCount"] ?? 0) + (int)item["AppCount"];
                    report["NeedAmount"] = ((decimal?)report["NeedAmount"] ?? 0) + (decimal)item["NeedAmount"];
                    report["Percent"] = ((decimal?)report["Percent"] ?? 0) + (decimal)item["Percent"];
                    report["NeedAmountPercent"] = ((decimal?)report["NeedAmountPercent"] ?? 0) + (decimal)item["NeedAmountPercent"];
                }
            }
            private static void FieldsInit(Doc item, ReportItem _item = null)//*
            {
                item["DocCount"] = (int?)item["DocCount"] ?? 0;
                item["AppCount"] = (int?)item["AppCount"] ?? 0;
                item["NeedAmount"] = (decimal?)item["NeedAmount"] ?? 0;
                item["Percent"] = (decimal?)item["Percent"] ?? 0;
                item["NeedAmountPercent"] = (decimal?)item["NeedAmountPercent"] ?? 0;
                item["PostSection1MonthCount"] = (int?)item["PostSection1MonthCount"] ?? 0;
                item["PostSection1AppCount"] = (int?)item["PostSection1AppCount"] ?? 0;
                item["PostSection1NeedAmount"] = (decimal?)item["PostSection1NeedAmount"] ?? 0;
                item["PostSection1Percent"] = (decimal?)item["PostSection1Percent"] ?? 0;
                item["PostSection2DocCount"] = (int?)item["PostSection2DocCount"] ?? 0;
                item["PostSection2AppCount"] = (int?)item["PostSection2AppCount"] ?? 0;
                item["PostSection2NeedAmount"] = (decimal?)item["PostSection2NeedAmount"] ?? 0;
                item["PostSection2Percent"] = (decimal?)item["PostSection2Percent"] ?? 0;
                item["PostAppCount"] = (int?)item["PostAppCount"] ?? 0;
                item["PostNeedAmount"] = (decimal?)item["PostNeedAmount"] ?? 0;
                item["PostPercent"] = (decimal?)item["PostPercent"] ?? 0;
                item["PostNeedAmountPercent"] = (decimal?)item["PostNeedAmountPercent"] ?? 0;

                item["BankSection1MonthCount"] = (int?)item["BankSection1MonthCount"] ?? 0;
                item["BankSection1AppCount"] = (int?)item["BankSection1AppCount"] ?? 0;
                item["BankSection1NeedAmount"] = (decimal?)item["BankSection1NeedAmount"] ?? 0;
                item["BankSection1Percent"] = (decimal?)item["BankSection1Percent"] ?? 0;
                item["BankSection2DocCount"] = (int?)item["BankSection2DocCount"] ?? 0;
                item["BankSection2AppCount"] = (int?)item["BankSection2AppCount"] ?? 0;
                item["BankSection2NeedAmount"] = (decimal?)item["BankSection2NeedAmount"] ?? 0;
                item["BankSection2Percent"] = (decimal?)item["BankSection2Percent"] ?? 0;
                item["BankAppCount"] = (int?)item["BankAppCount"] ?? 0;
                item["BankNeedAmount"] = (decimal?)item["BankNeedAmount"] ?? 0;
                item["BankPercent"] = (decimal?)item["BankPercent"] ?? 0;
                item["BankNeedAmountPercent"] = (decimal?)item["BankNeedAmountPercent"] ?? 0;
                if (_item != null)
                {
                    _item.DocCount = (int?)item["DocCount"] ?? 0;
                    _item.AppCount = (int?)item["AppCount"] ?? 0;
                    _item.NeedAmount = (decimal?)item["NeedAmount"] ?? 0;
                    _item.Percent = (decimal?)item["Percent"] ?? 0;
                    _item.NeedAmountPercent = (decimal?)item["NeedAmountPercent"] ?? 0;
                    _item.PostSection1MonthCount = (int?)item["PostSection1MonthCount"] ?? 0;
                    _item.PostSection1AppCount = (int?)item["PostSection1AppCount"] ?? 0;
                    _item.PostSection1NeedAmount = (decimal?)item["PostSection1NeedAmount"] ?? 0;
                    _item.PostSection1Percent = (decimal?)item["PostSection1Percent"] ?? 0;
                    _item.PostSection2DocCount = (int?)item["PostSection2DocCount"] ?? 0;
                    _item.PostSection2AppCount = (int?)item["PostSection2AppCount"] ?? 0;
                    _item.PostSection2NeedAmount = (decimal?)item["PostSection2NeedAmount"] ?? 0;
                    _item.PostSection2Percent = (decimal?)item["PostSection2Percent"] ?? 0;
                    _item.PostAppCount = (int?)item["PostAppCount"] ?? 0;
                    _item.PostNeedAmount = (decimal?)item["PostNeedAmount"] ?? 0;
                    _item.PostPercent = (decimal?)item["PostPercent"] ?? 0;
                    _item.PostNeedAmountPercent = (decimal?)item["PostNeedAmountPercent"] ?? 0;

                    _item.BankSection1MonthCount = (int?)item["BankSection1MonthCount"] ?? 0;
                    _item.BankSection1AppCount = (int?)item["BankSection1AppCount"] ?? 0;
                    _item.BankSection1NeedAmount = (decimal?)item["BankSection1NeedAmount"] ?? 0;
                    _item.BankSection1Percent = (decimal?)item["BankSection1Percent"] ?? 0;
                    _item.BankSection2DocCount = (int?)item["BankSection2DocCount"] ?? 0;
                    _item.BankSection2AppCount = (int?)item["BankSection2AppCount"] ?? 0;
                    _item.BankSection2NeedAmount = (decimal?)item["BankSection2NeedAmount"] ?? 0;
                    _item.BankSection2Percent = (decimal?)item["BankSection2Percent"] ?? 0;
                    _item.BankAppCount = (int?)item["BankAppCount"] ?? 0;
                    _item.BankNeedAmount = (decimal?)item["BankNeedAmount"] ?? 0;
                    _item.BankPercent = (decimal?)item["BankPercent"] ?? 0;
                    _item.BankNeedAmountPercent = (decimal?)item["BankNeedAmountPercent"] ?? 0;
                }
            }
            public class ReportItem
            {
                public string RowName { get; set; }
                public int DocCount { get; set; }
                public int AppCount { get; set; }
                public decimal NeedAmount { get; set; }
                public decimal Percent { get; set; }
                public decimal NeedAmountPercent { get; set; }

                public int PostSection1MonthCount { get; set; }
                public int PostSection1AppCount { get; set; }
                public decimal PostSection1NeedAmount { get; set; }
                public decimal PostSection1Percent { get; set; }
                public int PostSection2DocCount { get; set; }
                public int PostSection2AppCount { get; set; }
                public decimal PostSection2NeedAmount { get; set; }
                public decimal PostSection2Percent { get; set; }
                public int PostAppCount { get; set; }
                public decimal PostNeedAmount { get; set; }
                public decimal PostPercent { get; set; }
                public decimal PostNeedAmountPercent { get; set; }

                public int BankSection1MonthCount { get; set; }
                public int BankSection1AppCount { get; set; }
                public decimal BankSection1NeedAmount { get; set; }
                public decimal BankSection1Percent { get; set; }
                public int BankSection2DocCount { get; set; }
                public int BankSection2AppCount { get; set; }
                public decimal BankSection2NeedAmount { get; set; }
                public decimal BankSection2Percent { get; set; }
                public int BankAppCount { get; set; }
                public decimal BankNeedAmount { get; set; }
                public decimal BankPercent { get; set; }
                public decimal BankNeedAmountPercent { get; set; }
            }
            private static void CalcAppCount(Guid userId, Guid orderDefId, int year, int month, Guid orgId, List<Doc> items, string orderTypeName, decimal postPercent, WorkflowContext context, Guid sectionTypeId)
            {
                SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                var orderSrc = query.JoinSource(query.Source, orderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                //query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                query.AddCondition(ExpressionOperation.And, orderDefId, "&OrgId", ConditionOperation.Equal, orgId);
                query.AddCondition(ExpressionOperation.And, orderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                        canceledStateId, onApprovingStateId
                    });
                query.AddCondition(ExpressionOperation.And, orderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.In, new object[]
                {
                    retirementBenefitPaymentId, childBenefitPaymentId, invalidBenefitPaymentId, survivorBenefitPaymentId,
                    aidsFromBenefitPaymentId, aidsBenefitPaymentId, orphanBenefitPaymentId
                });
                query.AddAttribute(appSrc, "Assignments", SqlQuerySummaryFunction.Count);
                var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                query.AddGroupAttribute(appPaymentAttr);
                var table = new DataTable();
                using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }

                foreach (DataRow row in table.Rows)
                {
                    int count = row[0] is DBNull ? 0 : (int)row[0];
                    Guid categoryId;
                    Guid.TryParse(row[1].ToString(), out categoryId);
                    if (categoryId == Guid.Empty) continue;
                    try
                    {
                        var item = GetItemByCategory(items, categoryId);
                        var sNum = sectionTypeId == section1TypeId ? 1 : 2;
                        var field = orderTypeName + "Section" + sNum + "AppCount";
                        item[field] = ((int?)item[field] ?? 0) + count;
                    }
                    catch
                    {
                        throw new Exception(context.Enums.GetValue(categoryId).Value);
                        continue;
                    }
                }
                if (orderDefId != bankOrderDefId) CalcAppCount(userId, bankOrderDefId, year, month, orgId, items, "Bank", postPercent, context, sectionTypeId);
            }

            private static void CalcOrdersCount(Guid userId, Guid orderDefId, int year, int month, Guid orgId, List<Doc> items, string orderTypeName, WorkflowContext context)
            {
                SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                var orderSrc = query.JoinSource(query.Source, orderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                //query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, section2TypeId);
                query.AddCondition(ExpressionOperation.And, orderDefId, "&OrgId", ConditionOperation.Equal, orgId);
                query.AddCondition(ExpressionOperation.And, orderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                        canceledStateId, onApprovingStateId
                    });
                query.AddCondition(ExpressionOperation.And, orderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.In, new object[]
                {
                    retirementBenefitPaymentId, childBenefitPaymentId, invalidBenefitPaymentId, survivorBenefitPaymentId,
                    aidsFromBenefitPaymentId, aidsBenefitPaymentId, orphanBenefitPaymentId
                });
                query.AddAttribute(orderSrc, "&Id", SqlQuerySummaryFunction.Count);
                var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                query.AddGroupAttribute(appPaymentAttr);
                var table = new DataTable();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }
                foreach (DataRow row in table.Rows)
                {
                    int docCount = row[0] is DBNull ? 0 : (int)row[0];
                    Guid categoryId;
                    Guid.TryParse(row[1].ToString(), out categoryId);
                    if (categoryId == Guid.Empty) continue;
                    try
                    {
                        var item = GetItemByCategory(items, categoryId);
                        item[orderTypeName + "Section2DocCount"] = docCount;
                    }
                    catch
                    {
                        throw new Exception(context.Enums.GetValue(categoryId).Value);
                        continue;
                    }
                }
                if (orderDefId != bankOrderDefId)
                    CalcOrdersCount(userId, bankOrderDefId, year, month, orgId, items, "Bank", context);
            }
            private static void CalcAmountSum(Guid userId, int year, int month, Guid orgId, List<Doc> items, decimal postPercent, string orderTypeName, WorkflowContext context, Guid sectionTypeId)
            {
                var field = orderTypeName + "Section" + (sectionTypeId == section1TypeId ? 1 : 2);
                if (orderTypeName == "Post")
                {
                    SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                    var orderSrc = query.JoinSource(query.Source, postOrderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                    var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                    //query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                    query.AddCondition(ExpressionOperation.And, postOrderDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, postOrderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                        canceledStateId, onApprovingStateId
                    });
                    query.AddCondition(ExpressionOperation.And, postOrderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                    query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.In, new object[]
                    {
                        retirementBenefitPaymentId, childBenefitPaymentId, invalidBenefitPaymentId, survivorBenefitPaymentId,
                        aidsFromBenefitPaymentId, aidsBenefitPaymentId, orphanBenefitPaymentId
                    });
                    query.AddAttribute("Amount", SqlQuerySummaryFunction.Sum);
                    var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                    //query.AddAttribute("Amount");
                    //query.AddAttribute(appSrc, "RegNo");
                    //query.AddAttribute(appSrc, "PaymentSum");
                    query.AddGroupAttribute(appPaymentAttr);
                    var table = new DataTable();
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    string text = "";
                    decimal sum = 0m;
                    foreach (DataRow row in table.Rows)
                    {
                        decimal amount = row[0] is DBNull ? 0 : (decimal)row[0];
                        Guid categoryId;
                        Guid.TryParse(row[1].ToString(), out categoryId);
                        if (categoryId == Guid.Empty) continue;
                        /* if (categoryId == new Guid("{FDBDD774-EB88-46EA-9559-005D655BC196}"))//lj 18 лет
                         {                                                                   
                             var regNo = row[2] is DBNull ? "" : row[2].ToString();
                             decimal paySum = row[3] is DBNull ? 0 : (decimal)row[3];
                             text+= "[" + amount + ", " + regNo + ", сназ " + paySum + "]";
                             sum+= amount;           

                         } */
                        decimal service = (amount / 100) * postPercent;
                        try
                        {
                            var item = GetItemByCategory(items, categoryId);
                            item[field + "NeedAmount"] = ((decimal?)item[field + "NeedAmount"] ?? 0m) + amount;
                            item[field + "Percent"] = ((decimal?)item[field + "Percent"] ?? 0m) + service;
                        }
                        catch
                        {
                            continue;
                        }
                    }
                    CalcAmountSum(userId, year, month, orgId, items, postPercent, "Bank", context, sectionTypeId);
                }
                else
                {
                    SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                    var orderSrc = query.JoinSource(query.Source, bankOrderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                    var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                    var bankSrc = query.JoinSource(orderSrc, bankDefId, SqlSourceJoinType.Inner, "Bank");
                    //query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                    query.AddCondition(ExpressionOperation.And, bankOrderDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, bankOrderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                       canceledStateId, onApprovingStateId
                    });
                    query.AddCondition(ExpressionOperation.And, bankOrderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                    query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.In, new object[]
                    {
                    retirementBenefitPaymentId, childBenefitPaymentId, invalidBenefitPaymentId, survivorBenefitPaymentId,
                    aidsFromBenefitPaymentId, aidsBenefitPaymentId, orphanBenefitPaymentId
                    });
                    query.AddAttribute("Amount", SqlQuerySummaryFunction.Sum);
                    var bankAttr = query.AddAttribute(bankSrc, "Percent");
                    var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                    query.AddGroupAttribute(appPaymentAttr);
                    query.AddGroupAttribute(bankAttr);
                    var table = new DataTable();
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }

                    foreach (DataRow row in table.Rows)
                    {
                        decimal amount = row[0] is DBNull ? 0 : (decimal)row[0];
                        double percent;
                        double.TryParse(row[1].ToString(), out percent);
                        Guid categoryId;
                        Guid.TryParse(row[2].ToString(), out categoryId);
                        if (categoryId == Guid.Empty) continue;
                        decimal service = (amount / 100) * (decimal)percent;
                        try
                        {
                            var item = GetItemByCategory(items, categoryId);
                            item[field + "NeedAmount"] = ((decimal?)item[field + "NeedAmount"] ?? 0m) + amount;
                            item[field + "Percent"] = ((decimal?)item[field + "Percent"] ?? 0m) + service;
                        }
                        catch
                        {
                            continue;
                        }
                    }
                }
            }
            private static Doc GetItemByCategory(List<Doc> items, Guid categoryId)
            {
                if ((new[] { commonDeseas1CategoryId, hearDisabled1CategoryId, eyesDisabled1CategoryId }).Contains(categoryId))
                    return items[categoryTypes.IndexOf(commonDeseas1CategoryId)];
                else if ((new[] { commonDeseas2CategoryId, hearDisabled2CategoryId, eyesDisabled2CategoryId }).Contains(categoryId))
                    return items[categoryTypes.IndexOf(commonDeseas2CategoryId)];
                else if ((new[] { commonDeseas3CategoryId, hearDisabled3CategoryId, eyesDisabled3CategoryId }).Contains(categoryId))
                    return items[categoryTypes.IndexOf(commonDeseas3CategoryId)];
                else
                    return items[categoryTypes.IndexOf(categoryId)];
            }

            private static Dictionary<Guid, Decimal> GetPriceList(Guid userId, int month, int year, WorkflowContext context)
            {
                Dictionary<Guid, Decimal> catList = new Dictionary<Guid, Decimal>();
                QueryBuilder qb = new QueryBuilder(tariffDefId, userId);
                qb.Where("EffectiveDate").Le(new DateTime(year, month, 1)).And("ExpiryDate").Ge(new DateTime(year, month,
                DateTime.DaysInMonth(year, month)))
                .And("PaymentType").In(new object[]
            {
            retirementBenefitPaymentId, childBenefitPaymentId, invalidBenefitPaymentId, survivorBenefitPaymentId,
            aidsFromBenefitPaymentId, aidsBenefitPaymentId, orphanBenefitPaymentId
            });
                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("Category");
                query.AddAttribute("Amount");
                using (DataTable table = new DataTable())
                {
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    foreach (DataRow row in table.Rows)
                    {
                        Guid categoryId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                        if (categoryId != Guid.Empty)
                            if (!catList.ContainsKey(categoryId))
                                catList.Add(categoryId, row[1] is DBNull ? 0 : (decimal)row[1]);
                    }
                }
                return catList;
            }
            public static decimal GetPostPercent(int year, int month, Guid userId, WorkflowContext context)
            {
                SqlQuery query = new SqlQuery(context.DataContext, postDefId, userId);
                query.AddCondition(ExpressionOperation.And, postDefId, "DateFrom", ConditionOperation.LessEqual, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, postDefId, "DateTo", ConditionOperation.GreatEqual, new DateTime(year, month, DateTime.DaysInMonth(year, month)));
                query.AddAttribute("Size");
                using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    if (reader.Read()) return !reader.Reader.IsDBNull(0) ? reader.GetDecimal(0) : 0;
                return 0m;
            }
        }

        public static class LifetimeGrantReport1
        {
            // Виды выплат                             
            public static readonly Guid lifetimeGrantPayment = new Guid("{3DB68E47-2451-43E0-AB98-9A59C8B52686}");
            //Categories
            private static readonly Guid category1 = new Guid("{32B76FDF-145B-492C-A850-BE903B7AB6CA}");
            private static readonly Guid category2 = new Guid("{626357F4-5133-4426-A712-2E785E556F6E}");
            private static readonly Guid category3 = new Guid("{62960950-7E8F-420F-BCE6-D7F92CA4EA93}");
            private static readonly Guid category4 = new Guid("{677A9F5A-79F3-43F1-948E-AE10F6007677}");
            private static readonly Guid category5 = new Guid("{145C4F38-9BCF-44B3-95D0-EA1B8D1B869B}");
            private static readonly Guid category6 = new Guid("{C8B1465C-F377-4D9B-B23C-04FFC6836597}");
            private static readonly List<object> categoryTypes = new List<object>
            {
            category1,category2,category3,category4,category5,category6
            };
            // Document Defs Id 1e750c67-2ddf-488e-a4c4-d94547433067
            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");// "{04D25808-6DE9-42F5-8855-6F68A94A224C}"
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{86122C43-3E74-4A03-9F87-B29D1DA32961}");
            private static readonly Guid reportItemDefId = new Guid("{5251BDA5-6302-4086-ADE3-022C98F219DA}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");
            // States
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid canceledStateId = new Guid("{C65E63CC-54E9-424E-AFDC-5A8DB1CB04A3}"); // Аннулирован
            private static readonly Guid onApprovingStateId = new Guid("{5CD9E88D-671E-4A44-AD92-9F74DA3B47F7}"); //На утверждении
            private static readonly Guid onRegisteringStateId = new Guid("{A9CD37C4-A718-4DE1-9E95-EC8EC280C8D4}"); // На регистрации
            private static readonly Guid refusedStateId = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}"); // Отказан

            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {

                try
                {
                    var ui = context.GetUserInfo();
                    return Build(year, month, context.UserId, (Guid)ui.OrganizationId, context);
                }
                catch (Exception ex)
                {
                    throw new ApplicationException(ex.Message);
                }
            }

            private static Doc GetReportDoc(WorkflowContext context, List<object[]> reports)
            {
                Doc report;
                var docRepo = context.Documents;
                var approvedReportId = reports.FirstOrDefault(r => (Guid)r[1] == approvedStateId);
                if (approvedReportId != null)
                {
                    context["ApprovedReport"] = docRepo.LoadById((Guid)approvedReportId[0]);
                    return null;
                }
                reports = reports.Where(r => (Guid)r[1] == onRegisteringStateId).ToList();
                if (reports.Count > 0)
                {
                    var onRegisteringReportId = reports.FirstOrDefault(x => (DateTime)x[2] == reports.Max(r => (DateTime)r[2]));
                    report = docRepo.LoadById((Guid)onRegisteringReportId[0]);
                    reports.RemoveAll(x => (Guid)x[0] == (Guid)onRegisteringReportId[0]);
                    report["DocCount"] = null;
                    report["AppCount"] = null;
                    report["NeedAmount"] = null;
                    report["Percent"] = null;
                    report["NeedAmountPercent"] = null;
                    docRepo.ClearAttrDocList(report.Id, report.Get<DocListAttribute>("Rows").AttrDef.Id);
                    foreach (var reportId in reports.Select(x => (Guid)x[0]))
                    {
                        docRepo.SetDocState(reportId, refusedStateId);
                    }
                }
                else
                    report = docRepo.New(reportDefId);
                return report;
            }
            public static List<ReportItem> Build(int year, int month, Guid userId, Guid orgId, WorkflowContext context)
            {
                if (year < 2011 || year > 3000)
                    throw new ApplicationException("Ошибка в значении года!");
                if (month < 1 || month > 12)
                    throw new ApplicationException("Ошибка в значении месяца!");
                var docRepo = context.Documents;
                var qb = new QueryBuilder(reportDefId);
                qb.Where("&OrgId").Eq(orgId).And("Month").Eq(month).And("Year").Eq(year).And("&State").Neq(refusedStateId);

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("&Id");
                query.AddAttribute("&State");
                query.AddAttribute("&Created");
                var reports = new List<object[]>();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        reports.Add(new object[] { reader.GetGuid(0), reader.GetGuid(1), reader.GetDateTime(2) });
                    }
                }

                Dictionary<Guid, Decimal> prices = GetPriceList(userId, month, year, context);

                decimal postPercent = GetPostPercent(year, month, userId, context);
                List<Doc> items = new List<Doc>();
                var reportItemList = new List<ReportItem>();
                //Инициализация строк отчета
                foreach (Guid categoryId in categoryTypes)
                {
                    Doc item = docRepo.New(reportItemDefId);
                    item["Category"] = categoryId;
                    item["PaymentPrice"] = prices[categoryId];
                    reportItemList.Add(new ReportItem { Category = context.Enums.GetValue(categoryId).Value, PaymentPrice = prices[categoryId] });
                    items.Add(item);
                }

                Doc report = GetReportDoc(context, reports);
                if (report == null)
                {
                    return InitReport(context, context.Get<Doc>("ApprovedReport"), reportItemList);
                }

                CalcSection1(items, context, year, month, orgId, userId, postPercent);

                CalcSection2(items, context, year, month, orgId, userId, postPercent);

                CalcFields(items, report, reportItemList);

                items.Reverse();
                foreach (Doc item in items)
                {
                    docRepo.Save(item);
                    docRepo.AddDocToList(item.Id, report, "Rows");
                }
                report["Year"] = year;
                report["Month"] = month;
                report["Organization"] = orgId;

                docRepo.Save(report);
                docRepo.SetDocState(report, onRegisteringStateId);
                return reportItemList;
            }

            static List<ReportItem> InitReport(WorkflowContext context, Doc report, List<ReportItem> reportItemList)
            {
                var reportItems = new List<Doc>();
                var docRepo = context.Documents;
                foreach (var rItemId in docRepo.DocAttrList(out int c, report, "Rows", 0, 0))
                {
                    var curDocRepo = docRepo.LoadById(rItemId);
                    if (categoryTypes.Contains((Guid)curDocRepo["Category"]))
                        reportItems.Add(curDocRepo);
                }
                CalcFields(reportItems, report, reportItemList);
                return reportItemList;
            }
            private static void CalcSection1(List<Doc> items, WorkflowContext context, int year, int month, Guid orgId, Guid userId, decimal postPercent)
            {
                CalcAmountSum(userId, year, month, orgId, items, postPercent, "Post", context, section1TypeId);
            }

            private static void CalcSection2(List<Doc> items, WorkflowContext context, int year, int month, Guid orgId, Guid userId, decimal postPercent)
            {
                CalcAppCount(userId, postOrderDefId, year, month, orgId, items, "Post", postPercent, context, section2TypeId);
                CalcOrdersCount(userId, postOrderDefId, year, month, orgId, items, "Post", context);
                CalcAmountSum(userId, year, month, orgId, items, postPercent, "Post", context, section2TypeId);
            }

            private static void CalcFields(List<Doc> items, Doc report, List<ReportItem> _items)
            {
                int i = 0;
                foreach (var item in items)
                {
                    var _item = _items[i];
                    FieldsInit(item);
                    item["PostNeedAmount"] = (decimal)item["PostSection1NeedAmount"] + (decimal)item["PostSection2NeedAmount"];
                    item["PostPercent"] = (decimal)item["PostSection1Percent"] + (decimal)item["PostSection2Percent"];
                    item["PostNeedAmountPercent"] = (decimal)item["PostNeedAmount"] + (decimal)item["PostPercent"];

                    item["BankNeedAmount"] = (decimal)item["BankSection1NeedAmount"] + (decimal)item["BankSection2NeedAmount"];
                    item["BankPercent"] = (decimal)item["BankSection1Percent"] + (decimal)item["BankSection2Percent"];
                    item["BankNeedAmountPercent"] = (decimal)item["BankNeedAmount"] + (decimal)item["BankPercent"];

                    item["DocCount"] = (int)item["PostSection2DocCount"] + (int)item["BankSection2DocCount"];
                    item["AppCount"] = (int)item["PostSection2AppCount"] + (int)item["BankSection2AppCount"];
                    item["NeedAmount"] = (decimal)item["PostNeedAmount"] + (decimal)item["BankNeedAmount"];
                    item["Percent"] = (decimal)item["PostPercent"] + (decimal)item["BankPercent"];
                    item["NeedAmountPercent"] = (decimal)item["PostNeedAmountPercent"] + (decimal)item["BankNeedAmountPercent"];

                    report["DocCount"] = ((int?)report["DocCount"] ?? 0) + (int)item["DocCount"];
                    report["AppCount"] = ((int?)report["AppCount"] ?? 0) + (int)item["AppCount"];
                    report["NeedAmount"] = ((decimal?)report["NeedAmount"] ?? 0) + (decimal)item["NeedAmount"];
                    report["Percent"] = ((decimal?)report["Percent"] ?? 0) + (decimal)item["Percent"];
                    report["NeedAmountPercent"] = ((decimal?)report["NeedAmountPercent"] ?? 0) + (decimal)item["NeedAmountPercent"];
                    FieldsInit(item, _item);
                    i++;
                }
            }

            private static void FieldsInit(Doc item, ReportItem _item = null)
            {
                item["DocCount"] = (int?)item["DocCount"] ?? 0;
                item["AppCount"] = (int?)item["AppCount"] ?? 0;
                item["NeedAmount"] = (decimal?)item["NeedAmount"] ?? 0;
                item["Percent"] = (decimal?)item["Percent"] ?? 0;
                item["NeedAmountPercent"] = (decimal?)item["NeedAmountPercent"] ?? 0;
                item["PostSection1MonthCount"] = (int?)item["PostSection1MonthCount"] ?? 0;
                item["PostSection1AppCount"] = (int?)item["PostSection1AppCount"] ?? 0;
                item["PostSection1NeedAmount"] = (decimal?)item["PostSection1NeedAmount"] ?? 0;
                item["PostSection1Percent"] = (decimal?)item["PostSection1Percent"] ?? 0;
                item["PostSection2DocCount"] = (int?)item["PostSection2DocCount"] ?? 0;
                item["PostSection2AppCount"] = (int?)item["PostSection2AppCount"] ?? 0;
                item["PostSection2NeedAmount"] = (decimal?)item["PostSection2NeedAmount"] ?? 0;
                item["PostSection2Percent"] = (decimal?)item["PostSection2Percent"] ?? 0;
                item["PostAppCount"] = (int?)item["PostAppCount"] ?? 0;
                item["PostNeedAmount"] = (decimal?)item["PostNeedAmount"] ?? 0;
                item["PostPercent"] = (decimal?)item["PostPercent"] ?? 0;
                item["PostNeedAmountPercent"] = (decimal?)item["PostNeedAmountPercent"] ?? 0;

                item["BankSection1MonthCount"] = (int?)item["BankSection1MonthCount"] ?? 0;
                item["BankSection1AppCount"] = (int?)item["BankSection1AppCount"] ?? 0;
                item["BankSection1NeedAmount"] = (decimal?)item["BankSection1NeedAmount"] ?? 0;
                item["BankSection1Percent"] = (decimal?)item["BankSection1Percent"] ?? 0;
                item["BankSection2DocCount"] = (int?)item["BankSection2DocCount"] ?? 0;
                item["BankSection2AppCount"] = (int?)item["BankSection2AppCount"] ?? 0;
                item["BankSection2NeedAmount"] = (decimal?)item["BankSection2NeedAmount"] ?? 0;
                item["BankSection2Percent"] = (decimal?)item["BankSection2Percent"] ?? 0;
                item["BankAppCount"] = (int?)item["BankAppCount"] ?? 0;
                item["BankNeedAmount"] = (decimal?)item["BankNeedAmount"] ?? 0;
                item["BankPercent"] = (decimal?)item["BankPercent"] ?? 0;
                item["BankNeedAmountPercent"] = (decimal?)item["BankNeedAmountPercent"] ?? 0;
                if (_item != null)
                {
                    _item.DocCount = (int?)item["DocCount"] ?? 0;
                    _item.AppCount = (int?)item["AppCount"] ?? 0;
                    _item.NeedAmount = (decimal?)item["NeedAmount"] ?? 0;
                    _item.Percent = (decimal?)item["Percent"] ?? 0;
                    _item.NeedAmountPercent = (decimal?)item["NeedAmountPercent"] ?? 0;
                    _item.PostSection1MonthCount = (int?)item["PostSection1MonthCount"] ?? 0;
                    _item.PostSection1AppCount = (int?)item["PostSection1AppCount"] ?? 0;
                    _item.PostSection1NeedAmount = (decimal?)item["PostSection1NeedAmount"] ?? 0;
                    _item.PostSection1Percent = (decimal?)item["PostSection1Percent"] ?? 0;
                    _item.PostSection2DocCount = (int?)item["PostSection2DocCount"] ?? 0;
                    _item.PostSection2AppCount = (int?)item["PostSection2AppCount"] ?? 0;
                    _item.PostSection2NeedAmount = (decimal?)item["PostSection2NeedAmount"] ?? 0;
                    _item.PostSection2Percent = (decimal?)item["PostSection2Percent"] ?? 0;
                    _item.PostAppCount = (int?)item["PostAppCount"] ?? 0;
                    _item.PostNeedAmount = (decimal?)item["PostNeedAmount"] ?? 0;
                    _item.PostPercent = (decimal?)item["PostPercent"] ?? 0;
                    _item.PostNeedAmountPercent = (decimal?)item["PostNeedAmountPercent"] ?? 0;

                    _item.BankSection1MonthCount = (int?)item["BankSection1MonthCount"] ?? 0;
                    _item.BankSection1AppCount = (int?)item["BankSection1AppCount"] ?? 0;
                    _item.BankSection1NeedAmount = (decimal?)item["BankSection1NeedAmount"] ?? 0;
                    _item.BankSection1Percent = (decimal?)item["BankSection1Percent"] ?? 0;
                    _item.BankSection2DocCount = (int?)item["BankSection2DocCount"] ?? 0;
                    _item.BankSection2AppCount = (int?)item["BankSection2AppCount"] ?? 0;
                    _item.BankSection2NeedAmount = (decimal?)item["BankSection2NeedAmount"] ?? 0;
                    _item.BankSection2Percent = (decimal?)item["BankSection2Percent"] ?? 0;
                    _item.BankAppCount = (int?)item["BankAppCount"] ?? 0;
                    _item.BankNeedAmount = (decimal?)item["BankNeedAmount"] ?? 0;
                    _item.BankPercent = (decimal?)item["BankPercent"] ?? 0;
                    _item.BankNeedAmountPercent = (decimal?)item["BankNeedAmountPercent"] ?? 0;
                }
            }

            public class ReportItem
            {
                public int RowNo { get; set; }
                public string RowName { get; set; }
                public decimal Percent { get; set; }
                public string Category { get; set; }
                public int AppCount { get; set; }
                public decimal NeedAmountPercent { get; set; }
                public decimal PaymentPrice { get; set; }
                public decimal NeedAmount { get; set; }
                public int DocCount { get; set; }
                public int PostAppCount { get; set; }
                public decimal PostPercent { get; set; }
                public decimal PostNeedAmountPercent { get; set; }
                public decimal PostNeedAmount { get; set; }
                public int PostDocCount { get; set; }
                public int PostSection1DocCount { get; set; }
                public int PostSection1AppCount { get; set; }
                public decimal PostSection1Percent { get; set; }
                public decimal PostSection1NeedAmountPercent { get; set; }
                public int PostSection1MonthCount { get; set; }
                public decimal PostSection1NeedAmount { get; set; }
                public int PostSection2AppCount { get; set; }
                public int PostSection2MonthCount { get; set; }
                public decimal PostSection2NeedAmountPercent { get; set; }
                public decimal PostSection2Percent { get; set; }
                public int BankAppCount { get; set; }
                public int PostSection2DocCount { get; set; }
                public decimal PostSection2NeedAmount { get; set; }
                public int BankDocCount { get; set; }
                public decimal BankNeedAmountPercent { get; set; }
                public decimal BankPercent { get; set; }
                public decimal BankNeedAmount { get; set; }
                public int BankSection2DocCount { get; set; }
                public decimal BankSection2NeedAmount { get; set; }
                public decimal BankSection2NeedAmountPercent { get; set; }
                public int BankSection2MonthCount { get; set; }
                public decimal BankSection2Percent { get; set; }
                public int BankSection2AppCount { get; set; }
                public decimal BankSection1NeedAmount { get; set; }
                public int BankSection1DocCount { get; set; }
                public decimal BankSection1Percent { get; set; }
                public decimal BankSection1NeedAmountPercent { get; set; }
                public int BankSection1AppCount { get; set; }
                public int BankSection1MonthCount { get; set; }
            }

            private static void CalcAppCount(Guid userId, Guid orderDefId, int year, int month, Guid orgId, List<Doc> items, string orderTypeName, decimal postPercent, WorkflowContext context, Guid sectionTypeId)
            {
                SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                var orderSrc = query.JoinSource(query.Source, orderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                query.AddCondition(ExpressionOperation.And, orderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                query.AddCondition(ExpressionOperation.And, orderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, lifetimeGrantPayment);
                query.AddAttribute(appSrc, "Assignments", SqlQuerySummaryFunction.Count);
                var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                query.AddGroupAttribute(appPaymentAttr);
                var table = new DataTable();
                using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }

                foreach (DataRow row in table.Rows)
                {
                    int count = row[0] is DBNull ? 0 : (int)row[0];
                    Guid categoryId;
                    Guid.TryParse(row[1].ToString(), out categoryId);
                    if (categoryId == Guid.Empty)
                        throw new ApplicationException("Ошибка при чтении кол-ва чел. - " + orderTypeName + ". Категория не найдена.");
                    var item = items[categoryTypes.IndexOf(categoryId)];
                    var sNum = sectionTypeId == section1TypeId ? 1 : 2;
                    var field = orderTypeName + "Section" + sNum + "AppCount";
                    item[field] = ((int?)item[field] ?? 0) + count;
                }
                if (orderDefId != bankOrderDefId) CalcAppCount(userId, bankOrderDefId, year, month, orgId, items, "Bank", postPercent, context, sectionTypeId);
            }

            private static void CalcOrdersCount(Guid userId, Guid orderDefId, int year, int month, Guid orgId, List<Doc> items, string orderTypeName, WorkflowContext context)
            {
                SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                var orderSrc = query.JoinSource(query.Source, orderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, section2TypeId);
                query.AddCondition(ExpressionOperation.And, orderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                query.AddCondition(ExpressionOperation.And, orderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, lifetimeGrantPayment);
                query.AddAttribute(orderSrc, "&Id", SqlQuerySummaryFunction.Count);
                var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                query.AddGroupAttribute(appPaymentAttr);
                var table = new DataTable();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }
                foreach (DataRow row in table.Rows)
                {
                    int docCount = row[0] is DBNull ? 0 : (int)row[0];
                    Guid categoryId;
                    Guid.TryParse(row[1].ToString(), out categoryId);
                    if (categoryId == Guid.Empty) continue;
                    Doc item = items[categoryTypes.IndexOf(categoryId)];
                    item[orderTypeName + "Section2DocCount"] = docCount;
                }
                if (orderDefId != bankOrderDefId)
                    CalcOrdersCount(userId, bankOrderDefId, year, month, orgId, items, "Bank", context);
            }

            private static void CalcAmountSum(Guid userId, int year, int month, Guid orgId, List<Doc> items, decimal postPercent, string orderTypeName, WorkflowContext context, Guid sectionTypeId)
            {
                var field = orderTypeName + "Section" + (sectionTypeId == section1TypeId ? 1 : 2);
                if (orderTypeName == "Post")
                {
                    SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                    var orderSrc = query.JoinSource(query.Source, postOrderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                    var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                    query.AddCondition(ExpressionOperation.And, postOrderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                    query.AddCondition(ExpressionOperation.And, postOrderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                    query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, lifetimeGrantPayment);
                    query.AddAttribute("Amount", SqlQuerySummaryFunction.Sum);
                    var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                    query.AddGroupAttribute(appPaymentAttr);
                    var table = new DataTable();
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }

                    foreach (DataRow row in table.Rows)
                    {
                        decimal amount = row[0] is DBNull ? 0 : (decimal)row[0];
                        Guid categoryId;
                        Guid.TryParse(row[1].ToString(), out categoryId);
                        if (categoryId == Guid.Empty) continue;
                        decimal service = (amount / 100) * postPercent;

                        var item = items[categoryTypes.IndexOf(categoryId)];
                        item[field + "NeedAmount"] = ((decimal?)item[field + "NeedAmount"] ?? 0m) + amount;
                        item[field + "Percent"] = ((decimal?)item[field + "Percent"] ?? 0m) + service;
                    }
                    CalcAmountSum(userId, year, month, orgId, items, postPercent, "Bank", context, sectionTypeId);
                }
                else
                {
                    SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                    var orderSrc = query.JoinSource(query.Source, bankOrderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                    var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                    var bankSrc = query.JoinSource(orderSrc, bankDefId, SqlSourceJoinType.Inner, "Bank");
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                    query.AddCondition(ExpressionOperation.And, bankOrderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                    query.AddCondition(ExpressionOperation.And, bankOrderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                    query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, lifetimeGrantPayment);
                    query.AddAttribute("Amount", SqlQuerySummaryFunction.Sum);
                    var bankAttr = query.AddAttribute(bankSrc, "Percent");
                    var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                    query.AddGroupAttribute(appPaymentAttr);
                    query.AddGroupAttribute(bankAttr);
                    var table = new DataTable();
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }

                    foreach (DataRow row in table.Rows)
                    {
                        decimal amount = row[0] is DBNull ? 0 : (decimal)row[0];
                        double percent;
                        double.TryParse(row[1].ToString(), out percent);
                        Guid categoryId;
                        Guid.TryParse(row[2].ToString(), out categoryId);
                        if (categoryId == Guid.Empty) continue;
                        decimal service = (amount / 100) * (decimal)percent;

                        var item = items[categoryTypes.IndexOf(categoryId)];
                        item[field + "NeedAmount"] = ((decimal?)item[field + "NeedAmount"] ?? 0m) + amount;
                        item[field + "Percent"] = ((decimal?)item[field + "Percent"] ?? 0m) + service;
                    }
                }
            }

            private static Dictionary<Guid, Decimal> GetPriceList(Guid userId, int month, int year, WorkflowContext context)
            {
                Dictionary<Guid, Decimal> catList = new Dictionary<Guid, Decimal>();
                QueryBuilder qb = new QueryBuilder(tariffDefId, userId);
                qb.Where("EffectiveDate").Le(new DateTime(year, month, 1)).And("ExpiryDate").Ge(new DateTime(year, month,
                DateTime.DaysInMonth(year, month)))
                .And("PaymentType").Eq(lifetimeGrantPayment);
                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("Category");
                query.AddAttribute("Amount");
                using (DataTable table = new DataTable())
                {
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    foreach (DataRow row in table.Rows)
                    {
                        Guid categoryId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                        if (categoryId != Guid.Empty)
                            if (!catList.ContainsKey(categoryId))
                                catList.Add(categoryId, row[1] is DBNull ? 0 : (decimal)row[1]);
                            else
                                throw new ApplicationException("Ошибка при чтении тарифов ПС. Сообщите администрации об этой ошибке.");
                    }
                }
                return catList;
            }

            public static decimal GetPostPercent(int year, int month, Guid userId, WorkflowContext context)
            {
                SqlQuery query = new SqlQuery(context.DataContext, postDefId, userId);
                query.AddCondition(ExpressionOperation.And, postDefId, "DateFrom", ConditionOperation.LessEqual, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, postDefId, "DateTo", ConditionOperation.GreatEqual, new DateTime(year, month, DateTime.DaysInMonth(year, month)));
                query.AddAttribute("Size");
                using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    if (reader.Read()) return !reader.Reader.IsDBNull(0) ? reader.GetDecimal(0) : 0;
                return 0m;
            }
        }

        public static class AdditionalLifetimeGrantReport1
        {
            // Виды выплат                             
            public static readonly Guid additionalLifetimeGrantPayment = new Guid("{A72AA579-7F1A-4BCE-A6C0-A081B13AAD8F}");
            //Categories
            private static readonly Guid category1 = new Guid("{32B76FDF-145B-492C-A850-BE903B7AB6CA}");
            private static readonly Guid category2 = new Guid("{626357F4-5133-4426-A712-2E785E556F6E}");
            private static readonly Guid category3 = new Guid("{62960950-7E8F-420F-BCE6-D7F92CA4EA93}");
            private static readonly Guid category4 = new Guid("{677A9F5A-79F3-43F1-948E-AE10F6007677}");
            private static readonly Guid category5 = new Guid("{145C4F38-9BCF-44B3-95D0-EA1B8D1B869B}");
            private static readonly Guid category6 = new Guid("{C8B1465C-F377-4D9B-B23C-04FFC6836597}");
            private static readonly List<object> categoryTypes = new List<object>
            {
            category1,category2,category3,category4,category5,category6
            };
            // Document Defs Id 1e750c67-2ddf-488e-a4c4-d94547433067
            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");// "{04D25808-6DE9-42F5-8855-6F68A94A224C}"
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{AD72B200-60BA-412B-ADBF-F32D3BE2D40C}");
            private static readonly Guid reportItemDefId = new Guid("{F3FCDA28-FEBB-43A0-AD73-848BB175089E}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");
            // States
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid canceledStateId = new Guid("{C65E63CC-54E9-424E-AFDC-5A8DB1CB04A3}"); // Аннулирован
            private static readonly Guid onApprovingStateId = new Guid("{5CD9E88D-671E-4A44-AD92-9F74DA3B47F7}"); //На утверждении
            private static readonly Guid onRegisteringStateId = new Guid("{A9CD37C4-A718-4DE1-9E95-EC8EC280C8D4}"); // На регистрации
            private static readonly Guid refusedStateId = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}"); // Отказан

            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {

                try
                {
                    var ui = context.GetUserInfo();
                    return Build(year, month, context.UserId, (Guid)ui.OrganizationId, context);
                }
                catch (Exception ex)
                {
                    throw new ApplicationException(ex.Message);
                }
            }

            private static Doc GetReportDoc(WorkflowContext context, List<object[]> reports)
            {
                Doc report;
                var docRepo = context.Documents;
                var approvedReportId = reports.FirstOrDefault(r => (Guid)r[1] == approvedStateId);
                if (approvedReportId != null)
                {
                    context["ApprovedReport"] = docRepo.LoadById((Guid)approvedReportId[0]);
                    return null;
                }
                reports = reports.Where(r => (Guid)r[1] == onRegisteringStateId).ToList();
                if (reports.Count > 0)
                {
                    var onRegisteringReportId = reports.FirstOrDefault(x => (DateTime)x[2] == reports.Max(r => (DateTime)r[2]));
                    report = docRepo.LoadById((Guid)onRegisteringReportId[0]);
                    reports.RemoveAll(x => (Guid)x[0] == (Guid)onRegisteringReportId[0]);
                    report["DocCount"] = null;
                    report["AppCount"] = null;
                    report["NeedAmount"] = null;
                    report["Percent"] = null;
                    report["NeedAmountPercent"] = null;
                    docRepo.ClearAttrDocList(report.Id, report.Get<DocListAttribute>("Rows").AttrDef.Id);
                    foreach (var reportId in reports.Select(x => (Guid)x[0]))
                    {
                        docRepo.SetDocState(reportId, refusedStateId);
                    }
                }
                else
                    report = docRepo.New(reportDefId);
                return report;
            }

            public static List<ReportItem> Build(int year, int month, Guid userId, Guid orgId, WorkflowContext context)
            {
                if (year < 2011 || year > 3000)
                    throw new ApplicationException("Ошибка в значении года!");
                if (month < 1 || month > 12)
                    throw new ApplicationException("Ошибка в значении месяца!");
                var docRepo = context.Documents;
                var qb = new QueryBuilder(reportDefId);
                qb.Where("&OrgId").Eq(orgId).And("Month").Eq(month).And("Year").Eq(year).And("&State").Neq(refusedStateId);

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("&Id");
                query.AddAttribute("&State");
                query.AddAttribute("&Created");

                var reports = new List<object[]>();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        reports.Add(new object[] { reader.GetGuid(0), reader.GetGuid(1), reader.GetDateTime(2) });
                    }
                }

                var prices = GetPriceList(userId, month, year, context);

                var postPercent = GetPostPercent(year, month, userId, context);
                var items = new List<Doc>();
                var reportItemList = new List<ReportItem>();

                //Инициализация строк отчета
                foreach (Guid categoryId in categoryTypes)
                {
                    Doc item = docRepo.New(reportItemDefId);
                    item["Category"] = categoryId;
                    item["PaymentPrice"] = prices[categoryId];
                    reportItemList.Add(new ReportItem { Category = context.Enums.GetValue(categoryId).Value, PaymentPrice = prices[categoryId] });
                    items.Add(item);
                }

                Doc report = GetReportDoc(context, reports);
                if (report == null)
                {
                    return InitReport(context, context.Get<Doc>("ApprovedReport"), reportItemList);
                }


                CalcSection1(items, context, year, month, orgId, userId, postPercent);

                CalcSection2(items, context, year, month, orgId, userId, postPercent);

                CalcFields(items, report, reportItemList);

                items.Reverse();
                foreach (Doc item in items)
                {
                    docRepo.Save(item);
                    docRepo.AddDocToList(item.Id, report, "Rows");
                }
                report["Year"] = year;
                report["Month"] = month;
                report["Organization"] = orgId;

                docRepo.Save(report);
                docRepo.SetDocState(report, onRegisteringStateId);
                return reportItemList;
            }

            static List<ReportItem> InitReport(WorkflowContext context, Doc report, List<ReportItem> reportItemList)
            {
                var reportItems = new List<Doc>();
                var docRepo = context.Documents;
                foreach (var rItemId in docRepo.DocAttrList(out int c, report, "Rows", 0, 0))
                {
                    var curDocRepo = docRepo.LoadById(rItemId);
                    if (categoryTypes.Contains((Guid)curDocRepo["Category"]))
                        reportItems.Add(curDocRepo);
                }
                CalcFields(reportItems, report, reportItemList);
                return reportItemList;
            }

            private static void CalcSection1(List<Doc> items, WorkflowContext context, int year, int month, Guid orgId, Guid userId, decimal postPercent)
            {
                CalcAmountSum(userId, year, month, orgId, items, postPercent, "Post", context, section1TypeId);
            }

            private static void CalcSection2(List<Doc> items, WorkflowContext context, int year, int month, Guid orgId, Guid userId, decimal postPercent)
            {
                CalcAppCount(userId, postOrderDefId, year, month, orgId, items, "Post", postPercent, context, section2TypeId);
                CalcOrdersCount(userId, postOrderDefId, year, month, orgId, items, "Post", context);
                CalcAmountSum(userId, year, month, orgId, items, postPercent, "Post", context, section2TypeId);
            }

            private static void CalcFields(List<Doc> items, Doc report, List<ReportItem> _items)
            {
                int i = 0;
                foreach (var item in items)
                {
                    var _item = _items[i];
                    FieldsInit(item);
                    item["PostNeedAmount"] = (decimal)item["PostSection1NeedAmount"] + (decimal)item["PostSection2NeedAmount"];
                    item["PostPercent"] = (decimal)item["PostSection1Percent"] + (decimal)item["PostSection2Percent"];
                    item["PostNeedAmountPercent"] = (decimal)item["PostNeedAmount"] + (decimal)item["PostPercent"];

                    item["BankNeedAmount"] = (decimal)item["BankSection1NeedAmount"] + (decimal)item["BankSection2NeedAmount"];
                    item["BankPercent"] = (decimal)item["BankSection1Percent"] + (decimal)item["BankSection2Percent"];
                    item["BankNeedAmountPercent"] = (decimal)item["BankNeedAmount"] + (decimal)item["BankPercent"];

                    item["DocCount"] = (int)item["PostSection2DocCount"] + (int)item["BankSection2DocCount"];
                    item["AppCount"] = (int)item["PostSection2AppCount"] + (int)item["BankSection2AppCount"];
                    item["NeedAmount"] = (decimal)item["PostNeedAmount"] + (decimal)item["BankNeedAmount"];
                    item["Percent"] = (decimal)item["PostPercent"] + (decimal)item["BankPercent"];
                    item["NeedAmountPercent"] = (decimal)item["PostNeedAmountPercent"] + (decimal)item["BankNeedAmountPercent"];

                    report["DocCount"] = ((int?)report["DocCount"] ?? 0) + (int)item["DocCount"];
                    report["AppCount"] = ((int?)report["AppCount"] ?? 0) + (int)item["AppCount"];
                    report["NeedAmount"] = ((decimal?)report["NeedAmount"] ?? 0) + (decimal)item["NeedAmount"];
                    report["Percent"] = ((decimal?)report["Percent"] ?? 0) + (decimal)item["Percent"];
                    report["NeedAmountPercent"] = ((decimal?)report["NeedAmountPercent"] ?? 0) + (decimal)item["NeedAmountPercent"];
                    FieldsInit(item, _item);
                    i++;
                }
            }

            private static void FieldsInit(Doc item, ReportItem _item = null)
            {
                item["DocCount"] = (int?)item["DocCount"] ?? 0;
                item["AppCount"] = (int?)item["AppCount"] ?? 0;
                item["NeedAmount"] = (decimal?)item["NeedAmount"] ?? 0;
                item["Percent"] = (decimal?)item["Percent"] ?? 0;
                item["NeedAmountPercent"] = (decimal?)item["NeedAmountPercent"] ?? 0;
                item["PostSection1MonthCount"] = (int?)item["PostSection1MonthCount"] ?? 0;
                item["PostSection1AppCount"] = (int?)item["PostSection1AppCount"] ?? 0;
                item["PostSection1NeedAmount"] = (decimal?)item["PostSection1NeedAmount"] ?? 0;
                item["PostSection1Percent"] = (decimal?)item["PostSection1Percent"] ?? 0;
                item["PostSection2DocCount"] = (int?)item["PostSection2DocCount"] ?? 0;
                item["PostSection2AppCount"] = (int?)item["PostSection2AppCount"] ?? 0;
                item["PostSection2NeedAmount"] = (decimal?)item["PostSection2NeedAmount"] ?? 0;
                item["PostSection2Percent"] = (decimal?)item["PostSection2Percent"] ?? 0;
                item["PostAppCount"] = (int?)item["PostAppCount"] ?? 0;
                item["PostNeedAmount"] = (decimal?)item["PostNeedAmount"] ?? 0;
                item["PostPercent"] = (decimal?)item["PostPercent"] ?? 0;
                item["PostNeedAmountPercent"] = (decimal?)item["PostNeedAmountPercent"] ?? 0;

                item["BankSection1MonthCount"] = (int?)item["BankSection1MonthCount"] ?? 0;
                item["BankSection1AppCount"] = (int?)item["BankSection1AppCount"] ?? 0;
                item["BankSection1NeedAmount"] = (decimal?)item["BankSection1NeedAmount"] ?? 0;
                item["BankSection1Percent"] = (decimal?)item["BankSection1Percent"] ?? 0;
                item["BankSection2DocCount"] = (int?)item["BankSection2DocCount"] ?? 0;
                item["BankSection2AppCount"] = (int?)item["BankSection2AppCount"] ?? 0;
                item["BankSection2NeedAmount"] = (decimal?)item["BankSection2NeedAmount"] ?? 0;
                item["BankSection2Percent"] = (decimal?)item["BankSection2Percent"] ?? 0;
                item["BankAppCount"] = (int?)item["BankAppCount"] ?? 0;
                item["BankNeedAmount"] = (decimal?)item["BankNeedAmount"] ?? 0;
                item["BankPercent"] = (decimal?)item["BankPercent"] ?? 0;
                item["BankNeedAmountPercent"] = (decimal?)item["BankNeedAmountPercent"] ?? 0;
                if (_item != null)
                {
                    _item.DocCount = (int?)item["DocCount"] ?? 0;
                    _item.AppCount = (int?)item["AppCount"] ?? 0;
                    _item.NeedAmount = (decimal?)item["NeedAmount"] ?? 0;
                    _item.Percent = (decimal?)item["Percent"] ?? 0;
                    _item.NeedAmountPercent = (decimal?)item["NeedAmountPercent"] ?? 0;
                    _item.PostSection1MonthCount = (int?)item["PostSection1MonthCount"] ?? 0;
                    _item.PostSection1AppCount = (int?)item["PostSection1AppCount"] ?? 0;
                    _item.PostSection1NeedAmount = (decimal?)item["PostSection1NeedAmount"] ?? 0;
                    _item.PostSection1Percent = (decimal?)item["PostSection1Percent"] ?? 0;
                    _item.PostSection2DocCount = (int?)item["PostSection2DocCount"] ?? 0;
                    _item.PostSection2AppCount = (int?)item["PostSection2AppCount"] ?? 0;
                    _item.PostSection2NeedAmount = (decimal?)item["PostSection2NeedAmount"] ?? 0;
                    _item.PostSection2Percent = (decimal?)item["PostSection2Percent"] ?? 0;
                    _item.PostAppCount = (int?)item["PostAppCount"] ?? 0;
                    _item.PostNeedAmount = (decimal?)item["PostNeedAmount"] ?? 0;
                    _item.PostPercent = (decimal?)item["PostPercent"] ?? 0;
                    _item.PostNeedAmountPercent = (decimal?)item["PostNeedAmountPercent"] ?? 0;

                    _item.BankSection1MonthCount = (int?)item["BankSection1MonthCount"] ?? 0;
                    _item.BankSection1AppCount = (int?)item["BankSection1AppCount"] ?? 0;
                    _item.BankSection1NeedAmount = (decimal?)item["BankSection1NeedAmount"] ?? 0;
                    _item.BankSection1Percent = (decimal?)item["BankSection1Percent"] ?? 0;
                    _item.BankSection2DocCount = (int?)item["BankSection2DocCount"] ?? 0;
                    _item.BankSection2AppCount = (int?)item["BankSection2AppCount"] ?? 0;
                    _item.BankSection2NeedAmount = (decimal?)item["BankSection2NeedAmount"] ?? 0;
                    _item.BankSection2Percent = (decimal?)item["BankSection2Percent"] ?? 0;
                    _item.BankAppCount = (int?)item["BankAppCount"] ?? 0;
                    _item.BankNeedAmount = (decimal?)item["BankNeedAmount"] ?? 0;
                    _item.BankPercent = (decimal?)item["BankPercent"] ?? 0;
                    _item.BankNeedAmountPercent = (decimal?)item["BankNeedAmountPercent"] ?? 0;
                }
            }


            public class ReportItem
            {
                public int RowNo { get; set; }
                public string RowName { get; set; }
                public decimal Percent { get; set; }
                public string Category { get; set; }
                public int AppCount { get; set; }
                public decimal NeedAmountPercent { get; set; }
                public decimal PaymentPrice { get; set; }
                public decimal NeedAmount { get; set; }
                public int DocCount { get; set; }
                public int PostAppCount { get; set; }
                public decimal PostPercent { get; set; }
                public decimal PostNeedAmountPercent { get; set; }
                public decimal PostNeedAmount { get; set; }
                public int PostDocCount { get; set; }
                public int PostSection1DocCount { get; set; }
                public int PostSection1AppCount { get; set; }
                public decimal PostSection1Percent { get; set; }
                public decimal PostSection1NeedAmountPercent { get; set; }
                public int PostSection1MonthCount { get; set; }
                public decimal PostSection1NeedAmount { get; set; }
                public int PostSection2AppCount { get; set; }
                public int PostSection2MonthCount { get; set; }
                public decimal PostSection2NeedAmountPercent { get; set; }
                public decimal PostSection2Percent { get; set; }
                public int BankAppCount { get; set; }
                public int PostSection2DocCount { get; set; }
                public decimal PostSection2NeedAmount { get; set; }
                public int BankDocCount { get; set; }
                public decimal BankNeedAmountPercent { get; set; }
                public decimal BankPercent { get; set; }
                public decimal BankNeedAmount { get; set; }
                public int BankSection2DocCount { get; set; }
                public decimal BankSection2NeedAmount { get; set; }
                public decimal BankSection2NeedAmountPercent { get; set; }
                public int BankSection2MonthCount { get; set; }
                public decimal BankSection2Percent { get; set; }
                public int BankSection2AppCount { get; set; }
                public decimal BankSection1NeedAmount { get; set; }
                public int BankSection1DocCount { get; set; }
                public decimal BankSection1Percent { get; set; }
                public decimal BankSection1NeedAmountPercent { get; set; }
                public int BankSection1AppCount { get; set; }
                public int BankSection1MonthCount { get; set; }
            }

            private static void CalcAppCount(Guid userId, Guid orderDefId, int year, int month, Guid orgId, List<Doc> items, string orderTypeName, decimal postPercent, WorkflowContext context, Guid sectionTypeId)
            {
                SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                var orderSrc = query.JoinSource(query.Source, orderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                query.AddCondition(ExpressionOperation.And, orderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                query.AddCondition(ExpressionOperation.And, orderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, additionalLifetimeGrantPayment);
                query.AddAttribute(appSrc, "Assignments", SqlQuerySummaryFunction.Count);
                var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                query.AddGroupAttribute(appPaymentAttr);
                var table = new DataTable();
                using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }

                foreach (DataRow row in table.Rows)
                {
                    int count = row[0] is DBNull ? 0 : (int)row[0];
                    Guid categoryId;
                    Guid.TryParse(row[1].ToString(), out categoryId);
                    if (categoryId == Guid.Empty)
                        throw new ApplicationException("Ошибка при чтении кол-ва чел. - " + orderTypeName + ". Категория не найдена.");
                    Doc item;
                    try
                    {
                        item = items[categoryTypes.IndexOf(categoryId)];
                    }
                    catch
                    {
                        continue;
                    }
                    var sNum = sectionTypeId == section1TypeId ? 1 : 2;
                    var field = orderTypeName + "Section" + sNum + "AppCount";
                    item[field] = ((int?)item[field] ?? 0) + count;
                }
                if (orderDefId != bankOrderDefId) CalcAppCount(userId, bankOrderDefId, year, month, orgId, items, "Bank", postPercent, context, sectionTypeId);
            }

            private static void CalcOrdersCount(Guid userId, Guid orderDefId, int year, int month, Guid orgId, List<Doc> items, string orderTypeName, WorkflowContext context)
            {
                SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                var orderSrc = query.JoinSource(query.Source, orderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, section2TypeId);
                query.AddCondition(ExpressionOperation.And, orderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                query.AddCondition(ExpressionOperation.And, orderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, additionalLifetimeGrantPayment);
                query.AddAttribute(orderSrc, "&Id", SqlQuerySummaryFunction.Count);
                var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                query.AddGroupAttribute(appPaymentAttr);
                var table = new DataTable();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }
                foreach (DataRow row in table.Rows)
                {
                    int docCount = row[0] is DBNull ? 0 : (int)row[0];
                    Guid categoryId;
                    Guid.TryParse(row[1].ToString(), out categoryId);
                    if (categoryId == Guid.Empty) continue;
                    Doc item;
                    try
                    {
                        item = items[categoryTypes.IndexOf(categoryId)];
                    }
                    catch
                    {
                        continue;
                    }
                    item[orderTypeName + "Section2DocCount"] = docCount;
                }
                if (orderDefId != bankOrderDefId)
                    CalcOrdersCount(userId, bankOrderDefId, year, month, orgId, items, "Bank", context);
            }

            private static void CalcAmountSum(Guid userId, int year, int month, Guid orgId, List<Doc> items, decimal postPercent, string orderTypeName, WorkflowContext context, Guid sectionTypeId)
            {
                var field = orderTypeName + "Section" + (sectionTypeId == section1TypeId ? 1 : 2);
                if (orderTypeName == "Post")
                {
                    SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                    var orderSrc = query.JoinSource(query.Source, postOrderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                    var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                    query.AddCondition(ExpressionOperation.And, postOrderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                    query.AddCondition(ExpressionOperation.And, postOrderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                    query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, additionalLifetimeGrantPayment);
                    query.AddAttribute("Amount", SqlQuerySummaryFunction.Sum);

                    var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");

                    query.AddGroupAttribute(appPaymentAttr);
                    var table = new DataTable();
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }

                    foreach (DataRow row in table.Rows)
                    {
                        decimal amount = row[0] is DBNull ? 0 : (decimal)row[0];
                        Guid categoryId;
                        Guid.TryParse(row[1].ToString(), out categoryId);
                        if (categoryId == Guid.Empty) continue;
                        decimal service = (amount / 100) * postPercent;

                        Doc item;
                        try
                        {
                            item = items[categoryTypes.IndexOf(categoryId)];
                        }
                        catch
                        {
                            continue;
                        }
                        item[field + "NeedAmount"] = ((decimal?)item[field + "NeedAmount"] ?? 0m) + amount;
                        item[field + "Percent"] = ((decimal?)item[field + "Percent"] ?? 0m) + service;
                    }
                    CalcAmountSum(userId, year, month, orgId, items, postPercent, "Bank", context, sectionTypeId);
                }
                else
                {
                    SqlQuery query = new SqlQuery(context.DataContext, orderPaymentDefId, userId);
                    var orderSrc = query.JoinSource(query.Source, bankOrderDefId, SqlSourceJoinType.Inner, "OrderPayments");
                    var appSrc = query.JoinSource(orderSrc, appDefId, SqlSourceJoinType.Inner, "Application");
                    var bankSrc = query.JoinSource(orderSrc, bankDefId, SqlSourceJoinType.Inner, "Bank");
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Year", ConditionOperation.Equal, year);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Month", ConditionOperation.Equal, month);
                    query.AddCondition(ExpressionOperation.And, orderPaymentDefId, "Section", ConditionOperation.Equal, sectionTypeId);
                    query.AddCondition(ExpressionOperation.And, bankOrderDefId, "&State", ConditionOperation.NotIn, new object[]
                    {
                canceledStateId, onApprovingStateId
                    });
                    query.AddCondition(ExpressionOperation.And, bankOrderDefId, "ExpiryDate", ConditionOperation.GreatThen, new DateTime(year, month, 1));
                    query.AddCondition(ExpressionOperation.And, appDefId, "PaymentType", ConditionOperation.Equal, additionalLifetimeGrantPayment);
                    query.AddAttribute("Amount", SqlQuerySummaryFunction.Sum);
                    var bankAttr = query.AddAttribute(bankSrc, "Percent");
                    var appPaymentAttr = query.AddAttribute(appSrc, "PaymentCategory");
                    query.AddGroupAttribute(appPaymentAttr);
                    query.AddGroupAttribute(bankAttr);
                    var table = new DataTable();
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }

                    foreach (DataRow row in table.Rows)
                    {
                        decimal amount = row[0] is DBNull ? 0 : (decimal)row[0];
                        double percent;
                        double.TryParse(row[1].ToString(), out percent);
                        Guid categoryId;
                        Guid.TryParse(row[2].ToString(), out categoryId);
                        if (categoryId == Guid.Empty) continue;
                        decimal service = (amount / 100) * (decimal)percent;

                        Doc item;
                        try
                        {
                            item = items[categoryTypes.IndexOf(categoryId)];
                        }
                        catch
                        {
                            continue;
                        }
                        item[field + "NeedAmount"] = ((decimal?)item[field + "NeedAmount"] ?? 0m) + amount;
                        item[field + "Percent"] = ((decimal?)item[field + "Percent"] ?? 0m) + service;
                    }
                }
            }

            private static Dictionary<Guid, Decimal> GetPriceList(Guid userId, int month, int year, WorkflowContext context)
            {
                Dictionary<Guid, Decimal> catList = new Dictionary<Guid, Decimal>();
                QueryBuilder qb = new QueryBuilder(tariffDefId, userId);
                qb.Where("EffectiveDate").Le(new DateTime(year, month, 1)).And("ExpiryDate").Ge(new DateTime(year, month,
                DateTime.DaysInMonth(year, month)))
                .And("PaymentType").Eq(additionalLifetimeGrantPayment);
                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("Category");
                query.AddAttribute("Amount");
                using (DataTable table = new DataTable())
                {
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    foreach (DataRow row in table.Rows)
                    {
                        Guid categoryId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                        if (categoryId != Guid.Empty)
                            if (!catList.ContainsKey(categoryId))
                                catList.Add(categoryId, row[1] is DBNull ? 0 : (decimal)row[1]);
                            else
                                throw new ApplicationException("Ошибка при чтении тарифов ПС. Сообщите администрации об этой ошибке.");
                    }
                }
                return catList;
            }

            public static decimal GetPostPercent(int year, int month, Guid userId, WorkflowContext context)
            {
                SqlQuery query = new SqlQuery(context.DataContext, postDefId, userId);
                query.AddCondition(ExpressionOperation.And, postDefId, "DateFrom", ConditionOperation.LessEqual, new DateTime(year, month, 1));
                query.AddCondition(ExpressionOperation.And, postDefId, "DateTo", ConditionOperation.GreatEqual, new DateTime(year, month, DateTime.DaysInMonth(year, month)));
                query.AddAttribute("Size");
                using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    if (reader.Read()) return !reader.Reader.IsDBNull(0) ? reader.GetDecimal(0) : 0;
                return 0m;
            }
        }

        
        public static class MonthlySocialBenefits_1032 /*не акутально перевела на kgs*/
        {
            // Document Defs Id 1e750c67-2ddf-488e-a4c4-d94547433067
            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");// "{04D25808-6DE9-42F5-8855-6F68A94A224C}"
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{AD72B200-60BA-412B-ADBF-F32D3BE2D40C}");
            private static readonly Guid reportItemDefId = new Guid("{F3FCDA28-FEBB-43A0-AD73-848BB175089E}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");
            // States
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid canceledStateId = new Guid("{C65E63CC-54E9-424E-AFDC-5A8DB1CB04A3}"); // Аннулирован
            private static readonly Guid onApprovingStateId = new Guid("{5CD9E88D-671E-4A44-AD92-9F74DA3B47F7}"); //На утверждении
            private static readonly Guid onRegisteringStateId = new Guid("{A9CD37C4-A718-4DE1-9E95-EC8EC280C8D4}"); // На регистрации
            private static readonly Guid refusedStateId = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}"); // Отказан
            // Виды выплат
            private static readonly Guid retirementBenefitPaymentId = new Guid("{AFAA7F86-74AE-4260-9933-A56F7845E55A}");
            private static readonly Guid childBenefitPaymentId = new Guid("{AB3F8C41-897A-4574-BAA0-B7CD4AAA1C80}");
            private static readonly Guid invalidBenefitPaymentId = new Guid("{70C28E62-2387-4A59-917D-A366ADE119A8}");
            private static readonly Guid survivorBenefitPaymentId = new Guid("{839D5712-E75B-4E71-83F7-168CE4F089C0}");
            private static readonly Guid aidsFromBenefitPaymentId = new Guid("{3BEBE4F9-0B15-41CB-9B96-54E83819AB0F}");
            private static readonly Guid aidsBenefitPaymentId = new Guid("{47EEBFBC-A4E9-495D-A6A1-F87B5C3057C9}");
            private static readonly Guid orphanBenefitPaymentId = new Guid("{4F12C366-7E2F-4208-9CB8-4EAB6E6C0EF1}");

            private static readonly Guid alpineRetirementCategoryId = new Guid("{587D9992-DBB7-4BAD-A358-0FA571EBDB37}");
            private static readonly Guid retirementCategoryId = new Guid("{56DD1E0D-F693-470D-8756-6969DFA71A02}");
            private static readonly Guid heroMotherCategoryId = new Guid("{F21D59E6-BBAA-40D8-89FC-C7B3A707E8E6}");
            private static readonly Guid childDisableCategoryId = new Guid("{FDBDD774-EB88-46EA-9559-005D655BC196}");
            private static readonly Guid orphanChildrenCategoryId = new Guid("{401D9570-2A4E-453D-869E-9AA2603C9CD8}");
            private static readonly Guid childSurvivorsCategoryId = new Guid("{304A01B8-8A08-413E-B3D5-7B2C237829A2}");
            private static readonly Guid childFromAidsCategoryId = new Guid("{DE66F6D7-5462-4D45-8B20-6478728B5BD3}");
            private static readonly Guid childenAidsCategoryId = new Guid("{2222FD98-B885-4DC0-A0D4-271600AF281A}");
            private static readonly Guid childISPCategoryId = new Guid("{1E750C67-2DDF-488E-A4C4-D94547433067}");
            private static readonly Guid childhood1CategoryId = new Guid("{D18791C9-DE0A-4E15-92A8-20EF140C51ED}");
            private static readonly Guid childhood2CategoryId = new Guid("{305621EC-9ECC-4AF9-810D-5B639C339D50}");
            private static readonly Guid childhood3CategoryId = new Guid("{FD3B12FB-55D3-4229-975E-342AC126E942}");
            private static readonly Guid commonDeseas1CategoryId = new Guid("{409BCDA9-6770-4D3F-B515-7DE0E341C63D}");
            private static readonly Guid commonDeseas2CategoryId = new Guid("{0955ED04-8A32-476B-AE6E-E51DE0F2C66D}");
            private static readonly Guid commonDeseas3CategoryId = new Guid("{7B622DDA-D6C0-48CA-AFA9-7C74149D8BD5}");
            private static readonly Guid hearDisabled1CategoryId = new Guid("{C7B397DF-8325-4F0D-A94C-F8BD5E8152E4}");
            private static readonly Guid hearDisabled2CategoryId = new Guid("{18A54BA7-0330-40B3-AE9E-67D049004AB0}");
            private static readonly Guid hearDisabled3CategoryId = new Guid("{DB4E8C78-A17F-44FD-890D-A67F1EA2B457}");
            private static readonly Guid eyesDisabled1CategoryId = new Guid("{5EE97C99-B2A1-4759-9A28-876B08FE7BA8}");
            private static readonly Guid eyesDisabled2CategoryId = new Guid("{947E64F0-18EC-4A6B-80C0-FCECFE7C67C2}");
            private static readonly Guid eyesDisabled3CategoryId = new Guid("{8B787FCA-62EE-4261-9094-1FFAC1BF4C02}");

            public static List<object> categoryTypes = new List<object>
            {
             childDisableCategoryId,
             childISPCategoryId,
             childFromAidsCategoryId,
             childenAidsCategoryId,
             childhood1CategoryId,
             childhood2CategoryId,
             childhood3CategoryId,
             commonDeseas1CategoryId,
             commonDeseas2CategoryId,
             commonDeseas3CategoryId,
             childSurvivorsCategoryId,
             orphanChildrenCategoryId,
             alpineRetirementCategoryId,
             retirementCategoryId,
             heroMotherCategoryId
            };

            public static List<object> disabledChildrensGroup = new List<object>
            {
                childDisableCategoryId,
                childISPCategoryId,
                childFromAidsCategoryId,
                childenAidsCategoryId
            };

            public static List<object> childhoodGroup = new List<object>
            {
                childhood1CategoryId,
                childhood2CategoryId,
                childhood3CategoryId
            };
            public static List<object> commonDeseasesGroup = new List<object>
            {
                commonDeseas1CategoryId,
                childhood2CategoryId,
                childhood3CategoryId
            };

            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {
                var items = new List<ReportItem>();
                var fd = new DateTime(year, month, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var qb = new QueryBuilder(assignmentDefId, context.UserId);
                qb.Where("PaymentType").In(new object[]
                {
                    retirementBenefitPaymentId,
                    childBenefitPaymentId,
                    invalidBenefitPaymentId,
                    survivorBenefitPaymentId,
                    aidsFromBenefitPaymentId,
                    aidsBenefitPaymentId,
                    orphanBenefitPaymentId
                }).And("EffectiveDate").Lt(ld).And("ExpiryDate").Gt(fd);

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("&Id", SqlQuerySummaryFunction.Count);
                query.AddAttribute("Amount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute("&OrgId");
                query.AddAttribute("Category");
                query.AddGroupAttribute("&OrgId");
                query.AddGroupAttribute("Category");

                var list = new List<ReportItem>();

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        var count = reader.IsDbNull(0) ? 0 : reader.GetInt32(0);
                        var sum = reader.IsDbNull(1) ? 0m : reader.GetDecimal(1);
                        var orgId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                        var categoryId = reader.IsDbNull(3) ? Guid.Empty : reader.GetGuid(3);

                        if (orgId == Guid.Empty) continue;
                        var orgName = context.Orgs.GetOrgName(orgId);
                        var item = GetReportItem(items, orgName);
                        if (categoryTypes.Contains(categoryId))
                        {
                            string propertyName = convertCategoryToProperty(categoryId);
                            item = setValueByProperty(item, propertyName, count);
                            item.recipientSocialBenefitsTotal += count;
                            item.socialBenefitsAverage += sum;
                        }
                        if (disabledChildrensGroup.Contains(categoryId))
                            item.disabledChildrenTotal += count;
                        if (childhoodGroup.Contains(categoryId))
                            item.disabledSinceChildhoodTotal += count;
                        if (commonDeseasesGroup.Contains(categoryId))
                            item.disabledCommonDeseasesTotal += count;
                    }
                }
                foreach (ReportItem item in items)
                {
                    item.socialBenefitsAverage = Math.Round(item.socialBenefitsAverage / item.recipientSocialBenefitsTotal, 2);
                }
                return items;
            }
            private static ReportItem setValueByProperty(ReportItem item, string propertyName, int value)
            {
                if (!string.IsNullOrEmpty(propertyName))
                {
                    PropertyInfo propertyInfo = item.GetType().GetProperty(propertyName);
                    int prevValue = (int)propertyInfo.GetValue(item, null);
                    propertyInfo.SetValue(item, prevValue + value, null);
                }
                return item;
            }

            private static string convertCategoryToProperty(Guid categoryId)
            {
                if (categoryId.Equals(childDisableCategoryId))
                { return "childDisableCategory"; }
                else if (categoryId.Equals(childISPCategoryId))
                {
                    { return "childISPCategory"; }
                }
                else if (categoryId.Equals(childenAidsCategoryId))

                {
                    { return "childenAidsCategory"; }
                }
                else if (categoryId.Equals(childFromAidsCategoryId))

                {
                    { return "childFromAidsCategory"; }
                }
                else if (categoryId.Equals(childhood1CategoryId))
                {
                    { return "childhood1Category"; }
                }
                else if (categoryId.Equals(childhood2CategoryId))
                {
                    { return "childhood2Category"; }
                }
                else if (categoryId.Equals(childhood3CategoryId))
                {
                    { return "childhood3Category"; }
                }
                else if (categoryId.Equals(commonDeseas1CategoryId))
                {
                    { return "commonDeseas1Category"; }
                }
                else if (categoryId.Equals(commonDeseas2CategoryId))
                {
                    { return "commonDeseas2Category"; }
                }
                else if (categoryId.Equals(commonDeseas3CategoryId))
                {
                    { return "commonDeseas3Category"; }
                }
                else if (categoryId.Equals(childSurvivorsCategoryId))
                {
                    { return "childSurvivorsCategory"; }
                }
                else if (categoryId.Equals(orphanChildrenCategoryId))
                {
                    { return "orphanChildrenCategory"; }
                }
                else if (categoryId.Equals(alpineRetirementCategoryId))
                {
                    { return "alpineRetirementCategory"; }
                }
                else if (categoryId.Equals(retirementCategoryId))
                {
                    { return "retirementCategory"; }
                }
                else if (categoryId.Equals(heroMotherCategoryId))
                {
                    { return "heroMotherCategory"; }
                }
                else return "";
            }



            static ReportItem GetReportItem(List<ReportItem> items, string orgName)
            {
                var item = items.FirstOrDefault(x => x.orgName == orgName);
                if (item != null) return item;
                item = new ReportItem
                {
                    orgName = orgName,
                    rowNo = items.Count > 0 ? (items.Max(x => x.rowNo) + 1) : 1
                };
                items.Add(item);
                return item;
            }

            public class ReportItem
            {
                public int rowNo { get; set; }
                public string orgName { get; set; }
                public int childDisableCategory { get; set; }
                public int childISPCategory { get; set; }
                public int childFromAidsCategory { get; set; }
                public int childenAidsCategory { get; set; }
                public int disabledChildrenTotal { get; set; }
                public int childhood1Category { get; set; }
                public int childhood2Category { get; set; }
                public int childhood3Category { get; set; }
                public int disabledSinceChildhoodTotal { get; set; }
                public int commonDeseas1Category { get; set; }
                public int commonDeseas2Category { get; set; }
                public int commonDeseas3Category { get; set; }
                public int disabledCommonDeseasesTotal { get; set; }
                public int childSurvivorsCategory { get; set; }
                public int orphanChildrenCategory { get; set; }
                public int alpineRetirementCategory { get; set; }
                public int retirementCategory { get; set; }
                public int heroMotherCategory { get; set; }
                public int recipientSocialBenefitsTotal { get; set; }
                public decimal socialBenefitsAverage { get; set; }
                // public decimal Column20 { get; set; }
            }
        }

        public static class MonthlyFC_1001
        {
            private static readonly Guid privilege46PaymentId = new Guid("{7BEFD6DA-042C-4A77-90F3-A4424033E4DD}");

            private static readonly Guid reportDefId = new Guid("{4447EA34-67AB-46F2-BE03-A406CAC4EABC}"); //Заявка на финансирование ДК
            private static readonly Guid reportItemDefId = new Guid("{66605D33-A39E-4709-8534-C1505C041182}");
            private static readonly Guid reportShortDefId = new Guid("{5DC7BAD3-F6D7-4BF8-87BE-DA3D719917AC}"); //Строка заявки на финансирование ДК (краткая)
            private static readonly Guid reportItemShortDefId = new Guid("{7AC9E5A3-9FD3-4400-998B-7583FC130470}"); //Строка заявки на финансирование ДК (краткая)
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");
            // States
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static Guid cat1 = new Guid("{F080A1B0-9AA9-4339-B0B1-11D9541B28ED}");//Инвалиды ВОВ
            private static Guid cat2 = new Guid("{86114020-3577-46D4-85C2-7BB9D463FEFC}");//Участники ВОВ
            private static Guid cat3 = new Guid("{51377AAE-DD99-4CB9-82BF-E11FDE545147}");//Герои КР, Советского союза
            private static Guid cat4 = new Guid("{063FF85F-EFEE-4562-80A2-EE72C7442449}");//Несовершеннолетние узники концлагерей
            private static Guid cat5 = new Guid("{F7D81582-3F23-47F4-95CF-B4B9B275770B}");//Блокадники Ленинграда
            private static Guid cat6 = new Guid("{118CD71F-B219-47DA-B6F4-EA4467DD3B3A}");//Инвалиды СА
            private static Guid cat7 = new Guid("{1DF503C7-C53E-4892-A5E0-A1B956718A7B}");//Инвалиды-интернационалисты
            private static Guid cat8 = new Guid("{C11F38A5-5418-4455-9DCA-22F81513699B}");//Участники-интернационалисты
            private static Guid cat9 = new Guid("{55F9BB9F-4DBC-4DFA-9ED2-968C8F82E58A}");//Инвалиды ЧАЭС
            private static Guid cat10 = new Guid("{0224C18D-7654-4F92-B7E8-8B44C056F469}");//Участники ЧАЭС 1986-87 гг.
            private static Guid cat11 = new Guid("{4BD28541-094B-49A3-90B8-DD9FF6CFB997}");//Труж. тыла с группой инвалидности
            private static Guid cat12 = new Guid("{E7498511-A118-44B4-9407-E455F705515D}");//Трудармейцы
            private static Guid cat13 = new Guid("{32A216E3-081E-49C0-BBE2-4E7DCAA4F0FE}");//Реабилитированные граждане
            private static Guid cat14 = new Guid("{15613E81-FA2D-460C-A0DC-C9EA5FFA40A2}");//Участники ЧАЭС 1988-89 гг.
            private static Guid cat15 = new Guid("{3CE57669-237A-4CD2-8BAA-AA3FEFEB96C3}");//СП участников и умерших инв. ЧАЭС                 
            private static Guid cat16 = new Guid("{9BF497E9-04F9-4D02-81C1-7044A810B561}");//Дети до 18 лет участников ЧАЭС
            private static Guid cat17 = new Guid("{7FF0BFF7-CBF5-4ABB-899A-8F1A0A292A2A}");//Труженники тыла (без инвал.)
            private static Guid cat18 = new Guid("{A3EFED3D-0651-487D-AF4F-70D43508A47C}");//Семьи погибших военнослужащих
            private static Guid cat19 = new Guid("{B8497A2E-9C9F-4DEB-B521-8DDA3E7594D9}");//Семьи погибших воинов в ВОВ
            private static Guid cat20 = new Guid("{46A029B0-9A62-4E7D-A875-F2F33B1262EE}");//Семьи погибших сотрудников ОВД
            private static Guid cat21 = new Guid("{EFD864F1-F6CA-4CEE-A640-B7455396AE32}");//Вдовы ИВОВ
            private static Guid cat22 = new Guid("{DA73BA19-D8C6-449A-825E-D446523BD0EA}");//Вдовы УВОВ
            private static Guid cat23 = new Guid("{0A87F5D6-603F-4E26-B2FC-84812A8739FC}");//Вдовы блок. Ленинграда
            private static Guid cat24 = new Guid("{3A503598-29D8-4570-8250-969341E32B5A}");//Почетные доноры
            private static Guid cat25 = new Guid("{D2A3B9DD-120C-4597-A09A-C5F04B22B39E}");//Инвалиды по слуху и зрению
            private static Guid cat26 = new Guid("{C7FA81F8-D1BF-4371-9271-9BDA49AF16AE}");//В т.ч. инв. с детства по сл. и зр. до 18 лет
            private static Guid catTotal = new Guid("{419C6C7E-2790-486B-8A7B-0D6C2C61EF0A}");//Всего
            private static List<object> categoryTypes = new List<object>
            {
                cat1, cat2, cat3, cat4, cat5, cat6, cat7, cat8, cat9, cat10, cat11, cat12, cat13, cat14, cat15, cat16, cat17, cat18, cat19, cat20, cat21, cat22, cat23, cat24, cat25, cat26, catTotal
            };
            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {
                var items = new List<ReportItem>();
                var qb = new QueryBuilder(reportShortDefId);
                qb.Where("Original").Include("&State").Eq(approvedStateId).And("Year").Eq(year).And("Month").Eq(month).End();

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var itemDefSrc = query.JoinSource(query.Source, reportItemShortDefId, SqlSourceJoinType.Inner, "Rows");
                query.AddAttribute(query.Source, "&Id");
                query.AddAttribute(query.Source, "&OrgId");
                query.AddAttribute(itemDefSrc, "Category");                
                query.AddAttribute(itemDefSrc, "AppCount");
                //query.AddAttribute(itemDefSrc, "PaymentPrice");

                var tempList = new List<TempItem>();

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        var appId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                        var orgId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                        var categoryId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);                       
                        var appCount = reader.IsDbNull(3) ? 0 : reader.GetInt32(3);

                        if (orgId == Guid.Empty) continue;
                        tempList.Add(new TempItem
                        {
                            CategoryId = categoryId,
                            OrgId = orgId,                           
                            AppCount = appCount
                        });
                    }
                }
                Dictionary<Guid, Decimal> prices = GetPriceList(month, year, context);
                int counter = 1;
                //Инициализация строк отчета
                foreach (Guid categoryId in categoryTypes)
                {
                    if (!prices.ContainsKey(categoryId)) continue;
                    var item = new ReportItem
                    {
                        CategoryName = context.Enums.GetValue(categoryId).Value,
                        Price = prices[categoryId]                       
                    };

                    var subItems = tempList.Where(x => x.CategoryId == categoryId);  
                    item.Total = subItems.Sum(x => x.AppCount);
                    item.c_Bishkek = GetDetailRegion(subItems, bishkekList);
                    item.r_Chui = GetDetailRegion(subItems, chuiList);
                    item.r_Talas = GetDetailRegion(subItems, talasList);
                    item.r_IK = GetDetailRegion(subItems, issyk_kulList);
                    item.r_Naryn = GetDetailRegion(subItems, narynList);
                    item.r_Batken = GetDetailRegion(subItems, batkenList);
                    item.r_Osh = GetDetailRegion(subItems, oshList);
                    item.c_Osh = GetDetailRegion(subItems, osh);
                    item.r_JA = GetDetailRegion(subItems, jalal_abad);

                    item.RowNo = counter;
                    items.Add(item);
                    counter++;
                }
                return items;
            }
            private static Dictionary<Guid, Decimal> GetPriceList(int month, int year, WorkflowContext context)
            {
                Dictionary<Guid, Decimal> catList = new Dictionary<Guid, Decimal>();
                var qb = new QueryBuilder(reportShortDefId);
                qb.Where("Original").Include("&State").Eq(approvedStateId).And("Year").Eq(year).And("Month").Eq(month).End();

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var itemDefSrc = query.JoinSource(query.Source, reportItemShortDefId, SqlSourceJoinType.Inner, "Rows");                
                query.AddAttribute(itemDefSrc, "Category");
                query.AddAttribute(itemDefSrc, "PaymentPrice");
                using (DataTable table = new DataTable())
                {
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    foreach (DataRow row in table.Rows)
                    {
                        Guid categoryId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                        if (categoryId != Guid.Empty)
                            if (!catList.ContainsKey(categoryId))
                                catList.Add(categoryId, row[1] is DBNull ? 0m : (decimal)row[1]);
                    }
                }
                return catList;
            }
            private static Region GetDetailRegion(IEnumerable<TempItem> subItems, List<Guid> regionList)
            {
                Region region = new Region(regionList);
                foreach (RegionReportItem districtItem in region.RegionList)
                {
                    districtItem.AppCounter = subItems.Where(x => x.OrgId == districtItem.GetDistrictId()).Sum(x => x.AppCount);
                }
                region.RegionList = region.RegionList;
                region.RegionTotal = subItems.Where(x => regionList.Contains(x.OrgId)).Sum(x => x.AppCount);
                return region;
            }           
            public static List<Guid> bishkekList = new List<Guid>//г.Бишкек
            {                                
                //new Guid("{E59E153E-4FE9-4872-BBD0-9E183793EFEF}"),
                new Guid("{2AB93962-2A1F-42D4-9E70-78931B9A413D}"),//ленин
                new Guid("{34DDCAF2-EB08-48E7-894A-29C929D62C83}"),//первомайский
                new Guid("{0BADBCA1-ADD3-4B74-9A95-60F2EED92118}"),//свердлово
                new Guid("{17C0AC69-2247-41E8-B086-54599FE11CED}"),//октябрь
            };
            public static List<Guid> chuiList = new List<Guid>//Чуйская область
            {                                                    
                //new Guid("{41E34648-6C9D-44D0-8CA5-941DB051B163}"),
                new Guid("{78D008DF-F7E8-4F99-92A1-42E7AE6E34C3}"),//кемин
                new Guid("{AA466C30-47A6-4252-989D-F44C91A120A5}"),//чуй
                new Guid("{CAE58A37-FBE8-49AA-8E09-B167F871A5E8}"),//город токмок
                new Guid("{BEC45992-2D1C-4488-A260-FC7C5511F619}"),//ыссык-ата
                new Guid("{822E0311-F790-474D-BC97-E67CE8D14009}"),//аламудун
                new Guid("{C139D4DE-9F64-46D2-A908-1C477B78ECCD}"),//сокулук
                new Guid("{CA59DB1A-4AB2-4CB2-90C6-BF323E82E44A}"),//москва
                new Guid("{6612E6F7-BA2B-4E33-ADB9-FA0B14705E27}"),//жайыл
                new Guid("{72075FFB-371D-4C7B-8A5A-993E1C51CBFF}"),//панфилов
            };
            public static List<Guid> talasList = new List<Guid>//Таласская область
            {                                   
                //new Guid("{BC710978-8C0B-4063-8BA9-A12F75DE4829}"),
                new Guid("{02992EB8-E107-408E-954E-E322A73B433A}"),//г талас
                new Guid("{1E5868C3-B522-4DBF-BC8F-02C75A899C0F}"),//талас рн
                new Guid("{C285A1F2-2114-4752-A626-A235C6D6F2B1}"),//бакай-ата
                new Guid("{17A6CA38-8B7B-4F5C-973B-18F31646CC03}"),//кара-буура
                new Guid("{376FF578-372D-43A4-818E-31BC64327BEF}"),//манас
            };
            public static List<Guid> issyk_kulList = new List<Guid>//Иссык-Кульская область
            {
                //new Guid("{AD009808-FBAC-43CA-8671-5E790C213497}"),
                new Guid("{43842F64-7BB7-45EC-930B-54AD19186382}"),//каракол
                new Guid("{2FEA2DF5-24F6-4E53-BF6C-22604AC014C1}"),//балыкчы
                new Guid("{416366EE-580B-4E57-8641-0D90A2F5AB73}"),//иссык-куль
                new Guid("{5B438995-2070-46A8-9377-E922F0D64E4F}"),//тюп
                new Guid("{AAD91EAB-297F-40C8-B592-68573495EEC0}"),//жети-огуз
                new Guid("{55F9FFBF-B789-4002-BD64-B04163806897}"),//тон
                new Guid("{D2EDB83E-7EE5-4B5A-82A6-E9E49DD1E3B3}"),//ак-суу
            };
            public static List<Guid> narynList = new List<Guid>//Нарынская область
            {
                //new Guid("{CF4CA271-9202-4A5A-AE49-EB6237C33982}"),
                new Guid("{CB788BA9-8F94-4317-A655-B4F5527F3A4B}"),//акталаа
                new Guid("{4C60A2A2-73B4-4EA4-A6D0-E2B41370C7FC}"),//ат-баши
                new Guid("{52568E10-0210-4B09-B106-9E14520E26F1}"),//жумгал
                new Guid("{B631F3B0-1656-49C5-9152-0252F304D29B}"),//кочкор
                new Guid("{A6B9B397-81CF-43ED-A17B-6A13C110A678}"),//нарын район
                new Guid("{0A409F28-4A73-4CED-B368-10C6BE53419F}"),//нарын г
            };
            public static List<Guid> batkenList = new List<Guid>//Баткенская область
            {
                //new Guid("{4E98AD82-8ED4-4727-8E24-678921AB534C}"),
                new Guid("{0BCED873-4950-449A-98E6-69C44F64D70C}"),//баткен рн
                new Guid("{20745158-EAE3-434C-BDF5-5F893C8963ED}"),//баткен г
                new Guid("{E062B2A3-42C4-4619-B086-705E6C5367D3}"),//кадамжай
                new Guid("{F6CECF6A-2D5D-44C1-BC2F-29A836965531}"),//лейлек
                new Guid("{DDE31E57-4454-44EE-AFA9-80227E3C8620}"),//кызыл-кия
                new Guid("{E883C8F3-5E3C-4086-A078-91FD22E6550A}"),//сулюкта
            };
            public static List<Guid> oshList = new List<Guid>//Ошская область
            {
                //new Guid("{E01E9890-2A41-454E-9EC1-3D4AAF50ED62}"),
                new Guid("{C3C63817-FFB3-4905-BC70-8C3A3DBB67DC}"),//араван
                new Guid("{2A0481C0-FB12-4048-9153-CB7AE997A26C}"),//каракулджа
                new Guid("{3A305A9A-D30E-4C38-9E41-09DF993A658E}"),//кара-суу
                new Guid("{E0AF1DF7-AA64-45DD-873E-510CD413AD35}"),//ноокат
                new Guid("{B0B7490F-E48D-4DC7-9548-0505D73C858F}"),//узген рн
                new Guid("{2F8C280E-6C97-41D5-B24F-7D0DF4FF4C0E}"),//узген г
                new Guid("{5903E184-79E4-4630-AD91-FD488B84B832}"),//алай
                new Guid("{8A7D0C1C-A4A7-4582-8D65-8023CDB273E1}")//чон-алай
            };
            public static List<Guid> osh = new List<Guid>
            {
                new Guid ("{A99E469A-E8D4-4139-B89A-CE4AF6AA0733}")
            };
            public static List<Guid> jalal_abad = new List<Guid>//Джалал-Абадская область
            {
                //new Guid("{3ED50EFB-3E20-407C-93CB-9D8E0EF15B1B}"),
                new Guid("{D319DC07-E7F7-4997-AFCD-17CEAD707B7F}"),//г джалалабад
                new Guid("{8CAC9C70-D770-4AAB-8B0B-E1C432F6985A}"),//кок-жангак
                new Guid("{EFA2A70A-D10E-4941-B1F8-AE5218E5AD29}"),//майлысуу
                new Guid("{8E3CA117-0956-4F67-A8EF-B8222DC0A21E}"),//ташкумыр
                new Guid("{CB86E66E-8A41-4D92-9FD6-9BCF7E61F543}"),//каракуль
                new Guid("{9375DF49-1B41-4EFD-BB79-0FC2A86ABED6}"),//сузак
                new Guid("{A23770C6-2843-446E-8382-8C50AFD699E5}"),//базар-коргон
                new Guid("{A3E2A017-96F2-47C6-AF09-6FA4287E6953}"),//ноокен
                new Guid("{A5B70AEE-4E8B-4194-864D-F58E5BAB76E7}"),//аксы
                new Guid("{915D0BEB-53C1-440F-9CBA-00612369FEFC}"),//ала-бука
                new Guid("{5026031D-2BC9-4A24-8DB5-30C7DD992352}"),//чаткал
                new Guid("{F9D53DFB-0E18-44E6-A905-8B67F5751D37}"),//токтогул рн
                new Guid("{BF0A92AD-140E-4582-AB9F-DB304890C35A}")//тогуз-торо
            };
            public class TempItem
            {
                public Guid CategoryId { get; set; }
                public Guid OrgId { get; set; }
                public int AppCount { get; set; }
            }
            public class ReportItem
            {
                public int RowNo { get; set; }
                public string CategoryName { get; set; }
                public decimal Price { get; set; }
                public int Total { get; set; }
                public Region c_Bishkek { get; set; }
                public Region r_Chui { get; set; }
                public Region r_Talas { get; set; }
                public Region r_IK { get; set; }
                public Region r_Naryn { get; set; }
                public Region r_Batken { get; set; }
                public Region r_Osh { get; set; }
                public Region c_Osh { get; set; }
                public Region r_JA { get; set; }
            }

            [DataContract]
            public class Region
            {
                [DataMember]
                public int RegionTotal { get; set; }
                [DataMember]
                public List<RegionReportItem> RegionList { get; set; }

                public Region(List<Guid> _regionList)
                {
                    RegionList = new List<RegionReportItem>();
                    foreach (Guid districtId in _regionList)
                    {
                        RegionReportItem item = new RegionReportItem(districtId);
                        item.DistrictName = GetNameByCategoryId(districtId);
                        item.AppCounter = 0;
                        RegionList.Add(item);
                    }
                }
            }
            public static string GetNameByCategoryId(Guid regionGuid)
            {
                if (regionGuid.Equals(new Guid("{2AB93962-2A1F-42D4-9E70-78931B9A413D}")))
                    return "Ленинский";
                else if (regionGuid.Equals(new Guid("{34DDCAF2-EB08-48E7-894A-29C929D62C83}")))
                    return "Первомайский";
                else if (regionGuid.Equals(new Guid("{0BADBCA1-ADD3-4B74-9A95-60F2EED92118}")))
                    return "Свердловский";
                else if (regionGuid.Equals(new Guid("{17C0AC69-2247-41E8-B086-54599FE11CED}")))
                    return "Октябрьский";
                else if (regionGuid.Equals(new Guid("{78D008DF-F7E8-4F99-92A1-42E7AE6E34C3}")))
                    return "Кемин";
                else if (regionGuid.Equals(new Guid("{AA466C30-47A6-4252-989D-F44C91A120A5}")))
                    return "Чуй";
                else if (regionGuid.Equals(new Guid("{CAE58A37-FBE8-49AA-8E09-B167F871A5E8}")))
                    return "Токмок";
                else if (regionGuid.Equals(new Guid("{BEC45992-2D1C-4488-A260-FC7C5511F619}")))
                    return "Ыссык-Ата";
                else if (regionGuid.Equals(new Guid("{822E0311-F790-474D-BC97-E67CE8D14009}")))
                    return "Аламудун";
                else if (regionGuid.Equals(new Guid("{C139D4DE-9F64-46D2-A908-1C477B78ECCD}")))
                    return "Сокулук";
                else if (regionGuid.Equals(new Guid("{CA59DB1A-4AB2-4CB2-90C6-BF323E82E44A}")))
                    return "Москва";
                else if (regionGuid.Equals(new Guid("{6612E6F7-BA2B-4E33-ADB9-FA0B14705E27}")))
                    return "Жайыл";
                else if (regionGuid.Equals(new Guid("{72075FFB-371D-4C7B-8A5A-993E1C51CBFF}")))
                    return "Панфилов";
                else if (regionGuid.Equals(new Guid("{02992EB8-E107-408E-954E-E322A73B433A}")))
                    return "г.Талас";
                else if (regionGuid.Equals(new Guid("{1E5868C3-B522-4DBF-BC8F-02C75A899C0F}")))
                    return "Талас";
                else if (regionGuid.Equals(new Guid("{C285A1F2-2114-4752-A626-A235C6D6F2B1}")))
                    return "БакайАта";
                else if (regionGuid.Equals(new Guid("{17A6CA38-8B7B-4F5C-973B-18F31646CC03}")))
                    return "Кара-Буура";
                else if (regionGuid.Equals(new Guid("{376FF578-372D-43A4-818E-31BC64327BEF}")))
                    return "Манас";
                else if (regionGuid.Equals(new Guid("{43842F64-7BB7-45EC-930B-54AD19186382}")))
                    return "Каракол";
                else if (regionGuid.Equals(new Guid("{2FEA2DF5-24F6-4E53-BF6C-22604AC014C1}")))
                    return "Балыкчы";
                else if (regionGuid.Equals(new Guid("{416366EE-580B-4E57-8641-0D90A2F5AB73}")))
                    return "Ыссык-Куль";
                else if (regionGuid.Equals(new Guid("{5B438995-2070-46A8-9377-E922F0D64E4F}")))
                    return "Тюп";
                else if (regionGuid.Equals(new Guid("{AAD91EAB-297F-40C8-B592-68573495EEC0}")))
                    return "Жети-Огуз";
                else if (regionGuid.Equals(new Guid("{55F9FFBF-B789-4002-BD64-B04163806897}")))
                    return "Тон";
                else if (regionGuid.Equals(new Guid("{D2EDB83E-7EE5-4B5A-82A6-E9E49DD1E3B3}")))
                    return "Ак-Суу";
                else if (regionGuid.Equals(new Guid("{CB788BA9-8F94-4317-A655-B4F5527F3A4B}")))
                    return "Ак-Талаа";
                else if (regionGuid.Equals(new Guid("{4C60A2A2-73B4-4EA4-A6D0-E2B41370C7FC}")))
                    return "Ат-Баши";
                else if (regionGuid.Equals(new Guid("{52568E10-0210-4B09-B106-9E14520E26F1}")))
                    return "Жумгал";
                else if (regionGuid.Equals(new Guid("{B631F3B0-1656-49C5-9152-0252F304D29B}")))
                    return "Кочкор";
                else if (regionGuid.Equals(new Guid("{A6B9B397-81CF-43ED-A17B-6A13C110A678}")))
                    return "Нарын";
                else if (regionGuid.Equals(new Guid("{0A409F28-4A73-4CED-B368-10C6BE53419F}")))
                    return "г.Нарын";
                else if (regionGuid.Equals(new Guid("{0BCED873-4950-449A-98E6-69C44F64D70C}")))
                    return "Баткен";
                else if (regionGuid.Equals(new Guid("{20745158-EAE3-434C-BDF5-5F893C8963ED}")))
                    return "г.Баткен";
                else if (regionGuid.Equals(new Guid("{E062B2A3-42C4-4619-B086-705E6C5367D3}")))
                    return "Кадамжай";
                else if (regionGuid.Equals(new Guid("{F6CECF6A-2D5D-44C1-BC2F-29A836965531}")))
                    return "Лейлек";
                else if (regionGuid.Equals(new Guid("{DDE31E57-4454-44EE-AFA9-80227E3C8620}")))
                    return "Кызыл-Кия";
                else if (regionGuid.Equals(new Guid("{E883C8F3-5E3C-4086-A078-91FD22E6550A}")))
                    return "Сулюкта";
                else if (regionGuid.Equals(new Guid("{C3C63817-FFB3-4905-BC70-8C3A3DBB67DC}")))
                    return "Араван";
                else if (regionGuid.Equals(new Guid("{2A0481C0-FB12-4048-9153-CB7AE997A26C}")))
                    return "Кара-Кулджа";
                else if (regionGuid.Equals(new Guid("{3A305A9A-D30E-4C38-9E41-09DF993A658E}")))
                    return "Кара-Суу";
                else if (regionGuid.Equals(new Guid("{E0AF1DF7-AA64-45DD-873E-510CD413AD35}")))
                    return "Ноокат";
                else if (regionGuid.Equals(new Guid("{B0B7490F-E48D-4DC7-9548-0505D73C858F}")))
                    return "Узген";
                else if (regionGuid.Equals(new Guid("{2F8C280E-6C97-41D5-B24F-7D0DF4FF4C0E}")))
                    return "г.Узген";
                else if (regionGuid.Equals(new Guid("{5903E184-79E4-4630-AD91-FD488B84B832}")))
                    return "Алай";
                else if (regionGuid.Equals(new Guid("{8A7D0C1C-A4A7-4582-8D65-8023CDB273E1}")))
                    return "Чон-Алай";
                else if (regionGuid.Equals(new Guid("{A99E469A-E8D4-4139-B89A-CE4AF6AA0733}")))
                    return "г.Ош";
                else if (regionGuid.Equals(new Guid("{D319DC07-E7F7-4997-AFCD-17CEAD707B7F}")))
                    return "г.Джалал-Абад";
                else if (regionGuid.Equals(new Guid("{8CAC9C70-D770-4AAB-8B0B-E1C432F6985A}")))
                    return "Кок-Жангак";
                else if (regionGuid.Equals(new Guid("{EFA2A70A-D10E-4941-B1F8-AE5218E5AD29}")))
                    return "Майлы-Суу";
                else if (regionGuid.Equals(new Guid("{8E3CA117-0956-4F67-A8EF-B8222DC0A21E}")))
                    return "Таш-Кумыр";
                else if (regionGuid.Equals(new Guid("{CB86E66E-8A41-4D92-9FD6-9BCF7E61F543}")))
                    return "Кара-Куль";
                else if (regionGuid.Equals(new Guid("{9375DF49-1B41-4EFD-BB79-0FC2A86ABED6}")))
                    return "Сузак";
                else if (regionGuid.Equals(new Guid("{A23770C6-2843-446E-8382-8C50AFD699E5}")))
                    return "Базар-Коргон";
                else if (regionGuid.Equals(new Guid("{A3E2A017-96F2-47C6-AF09-6FA4287E6953}")))
                    return "Ноокен";
                else if (regionGuid.Equals(new Guid("{A5B70AEE-4E8B-4194-864D-F58E5BAB76E7}")))
                    return "Аксы";
                else if (regionGuid.Equals(new Guid("{915D0BEB-53C1-440F-9CBA-00612369FEFC}")))
                    return "Ала-Бука";
                else if (regionGuid.Equals(new Guid("{5026031D-2BC9-4A24-8DB5-30C7DD992352}")))
                    return "Чаткал";
                else if (regionGuid.Equals(new Guid("{F9D53DFB-0E18-44E6-A905-8B67F5751D37}")))
                    return "Токтогул";
                else if (regionGuid.Equals(new Guid("{BF0A92AD-140E-4582-AB9F-DB304890C35A}")))
                    return "Тогуз-Торо";
                else return "";
            }

            [DataContract]
            public class RegionReportItem
            {
                public RegionReportItem(Guid districtId)
                {
                    DistrictId = districtId;
                }
                [DataMember]
                public string DistrictName { get; set; }
                private Guid DistrictId { get; set; }
                public Guid GetDistrictId()
                {
                    return DistrictId;
                }
                [DataMember]
                public int AppCounter { get; set; }
            }
        }     

        public static class MonthlyDK_1002
        {
            private static readonly Guid privilege46PaymentId = new Guid("{7BEFD6DA-042C-4A77-90F3-A4424033E4DD}");

            private static readonly Guid reportDefId = new Guid("{4447EA34-67AB-46F2-BE03-A406CAC4EABC}"); //Заявка на финансирование ДК
            private static readonly Guid reportItemDefId = new Guid("{66605D33-A39E-4709-8534-C1505C041182}");
            private static readonly Guid reportShortDefId = new Guid("{5DC7BAD3-F6D7-4BF8-87BE-DA3D719917AC}"); //Строка заявки на финансирование ДК (краткая)
            private static readonly Guid reportItemShortDefId = new Guid("{7AC9E5A3-9FD3-4400-998B-7583FC130470}"); //Строка заявки на финансирование ДК (краткая)
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");
            // States
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static Guid cat1 = new Guid("{F080A1B0-9AA9-4339-B0B1-11D9541B28ED}");//Инвалиды ВОВ
            private static Guid cat2 = new Guid("{86114020-3577-46D4-85C2-7BB9D463FEFC}");//Участники ВОВ
            private static Guid cat3 = new Guid("{51377AAE-DD99-4CB9-82BF-E11FDE545147}");//Герои КР, Советского союза
            private static Guid cat4 = new Guid("{063FF85F-EFEE-4562-80A2-EE72C7442449}");//Несовершеннолетние узники концлагерей
            private static Guid cat5 = new Guid("{F7D81582-3F23-47F4-95CF-B4B9B275770B}");//Блокадники Ленинграда
            private static Guid cat6 = new Guid("{118CD71F-B219-47DA-B6F4-EA4467DD3B3A}");//Инвалиды СА
            private static Guid cat7 = new Guid("{1DF503C7-C53E-4892-A5E0-A1B956718A7B}");//Инвалиды-интернационалисты
            private static Guid cat8 = new Guid("{C11F38A5-5418-4455-9DCA-22F81513699B}");//Участники-интернационалисты
            private static Guid cat9 = new Guid("{55F9BB9F-4DBC-4DFA-9ED2-968C8F82E58A}");//Инвалиды ЧАЭС
            private static Guid cat10 = new Guid("{0224C18D-7654-4F92-B7E8-8B44C056F469}");//Участники ЧАЭС 1986-87 гг.
            private static Guid cat11 = new Guid("{4BD28541-094B-49A3-90B8-DD9FF6CFB997}");//Труж. тыла с группой инвалидности
            private static Guid cat12 = new Guid("{E7498511-A118-44B4-9407-E455F705515D}");//Трудармейцы
            private static Guid cat13 = new Guid("{32A216E3-081E-49C0-BBE2-4E7DCAA4F0FE}");//Реабилитированные граждане
            private static Guid cat14 = new Guid("{15613E81-FA2D-460C-A0DC-C9EA5FFA40A2}");//Участники ЧАЭС 1988-89 гг.
            private static Guid cat15 = new Guid("{3CE57669-237A-4CD2-8BAA-AA3FEFEB96C3}");//СП участников и умерших инв. ЧАЭС                 
            private static Guid cat16 = new Guid("{9BF497E9-04F9-4D02-81C1-7044A810B561}");//Дети до 18 лет участников ЧАЭС
            private static Guid cat17 = new Guid("{7FF0BFF7-CBF5-4ABB-899A-8F1A0A292A2A}");//Труженники тыла (без инвал.)
            private static Guid cat18 = new Guid("{A3EFED3D-0651-487D-AF4F-70D43508A47C}");//Семьи погибших военнослужащих
            private static Guid cat19 = new Guid("{B8497A2E-9C9F-4DEB-B521-8DDA3E7594D9}");//Семьи погибших воинов в ВОВ
            private static Guid cat20 = new Guid("{46A029B0-9A62-4E7D-A875-F2F33B1262EE}");//Семьи погибших сотрудников ОВД
            private static Guid cat21 = new Guid("{EFD864F1-F6CA-4CEE-A640-B7455396AE32}");//Вдовы ИВОВ
            private static Guid cat22 = new Guid("{DA73BA19-D8C6-449A-825E-D446523BD0EA}");//Вдовы УВОВ
            private static Guid cat23 = new Guid("{0A87F5D6-603F-4E26-B2FC-84812A8739FC}");//Вдовы блок. Ленинграда
            private static Guid cat24 = new Guid("{3A503598-29D8-4570-8250-969341E32B5A}");//Почетные доноры
            private static Guid cat25 = new Guid("{D2A3B9DD-120C-4597-A09A-C5F04B22B39E}");//Инвалиды по слуху и зрению
            private static Guid cat26 = new Guid("{C7FA81F8-D1BF-4371-9271-9BDA49AF16AE}");//В т.ч. инв. с детства по сл. и зр. до 18 лет
            private static Guid catTotal = new Guid("{419C6C7E-2790-486B-8A7B-0D6C2C61EF0A}");//Всего
            private static List<object> categoryTypes = new List<object>
            {
                cat1, cat2, cat3, cat4, cat5, cat6, cat7, cat8, cat9, cat10, cat11, cat12, cat13, cat14, cat15, cat16, cat17, cat18, cat19, cat20, cat21, cat22, cat23, cat24, cat25, cat26, catTotal
            };
            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {
                var items = new List<ReportItem>();
                var qb = new QueryBuilder(reportShortDefId);
                qb.Where("Original").Include("&State").Eq(approvedStateId).And("Year").Eq(year).And("Month").Eq(month).End();

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var itemDefSrc = query.JoinSource(query.Source, reportItemShortDefId, SqlSourceJoinType.Inner, "Rows");
                query.AddAttribute(query.Source, "&Id");
                query.AddAttribute(query.Source, "&OrgId");
                query.AddAttribute(itemDefSrc, "Category");
                query.AddAttribute(itemDefSrc, "AppCount");
                //query.AddAttribute(itemDefSrc, "PaymentPrice");

                var tempList = new List<TempItem>();

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        var appId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                        var orgId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                        var categoryId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                        var appCount = reader.IsDbNull(3) ? 0 : reader.GetInt32(3);

                        if (orgId == Guid.Empty) continue;
                        tempList.Add(new TempItem
                        {
                            CategoryId = categoryId,
                            OrgId = orgId,
                            AppCount = appCount
                        });
                    }
                }
                Dictionary<Guid, Decimal> prices = GetPriceList(month, year, context);
                int counter = 1;
                //Инициализация строк отчета
                foreach (Guid categoryId in categoryTypes)
                {

                    if (!prices.ContainsKey(categoryId)) continue;
                    var item = new ReportItem
                    {
                        CategoryName = context.Enums.GetValue(categoryId).Value,
                        Price = prices[categoryId]
                    };
                    var subItems = tempList.Where(x => x.CategoryId == categoryId);

                    item.Total = subItems.Sum(x => x.AppCount);

                    item.c_Bishkek = subItems.Where(x => bishkekList.Contains(x.OrgId)).Sum(x => x.AppCount);

                    item.r_Chui = subItems.Where(x => chuiList.Contains(x.OrgId)).Sum(x => x.AppCount);

                    item.r_Talas = subItems.Where(x => talasList.Contains(x.OrgId)).Sum(x => x.AppCount);

                    item.r_IK = subItems.Where(x => issyk_kulList.Contains(x.OrgId)).Sum(x => x.AppCount);

                    item.r_Naryn = subItems.Where(x => narynList.Contains(x.OrgId)).Sum(x => x.AppCount);

                    item.r_Batken = subItems.Where(x => batkenList.Contains(x.OrgId)).Sum(x => x.AppCount);

                    item.r_Osh = subItems.Where(x => oshList.Contains(x.OrgId)).Sum(x => x.AppCount);

                    item.c_Osh = subItems.Where(x => x.OrgId == osh).Sum(x => x.AppCount);

                    item.r_JA = subItems.Where(x => jalal_abad.Contains(x.OrgId)).Sum(x => x.AppCount);

                    item.RowNo = counter;

                    items.Add(item);
                    counter++;
                }
                return items;
            }
            private static Dictionary<Guid, Decimal> GetPriceList(int month, int year, WorkflowContext context)
            {
                Dictionary<Guid, Decimal> catList = new Dictionary<Guid, Decimal>();
                var qb = new QueryBuilder(reportShortDefId);
                qb.Where("Original").Include("&State").Eq(approvedStateId).And("Year").Eq(year).And("Month").Eq(month).End();

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var itemDefSrc = query.JoinSource(query.Source, reportItemShortDefId, SqlSourceJoinType.Inner, "Rows");
                query.AddAttribute(itemDefSrc, "Category");
                query.AddAttribute(itemDefSrc, "PaymentPrice");
                using (DataTable table = new DataTable())
                {
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    foreach (DataRow row in table.Rows)
                    {
                        Guid categoryId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                        if (categoryId != Guid.Empty)
                            if (!catList.ContainsKey(categoryId))
                                catList.Add(categoryId, row[1] is DBNull ? 0m : (decimal)row[1]);
                    }
                }
                return catList;
            }

            private static List<Guid> bishkekList = new List<Guid>//г.Бишкек
            {                                
                //new Guid("{E59E153E-4FE9-4872-BBD0-9E183793EFEF}"),
                new Guid("{2AB93962-2A1F-42D4-9E70-78931B9A413D}"),//ленин
                new Guid("{34DDCAF2-EB08-48E7-894A-29C929D62C83}"),//первомайский
                new Guid("{0BADBCA1-ADD3-4B74-9A95-60F2EED92118}"),//свердлово
                new Guid("{17C0AC69-2247-41E8-B086-54599FE11CED}"),//октябрь
            };
            private static List<Guid> chuiList = new List<Guid>//Чуйская область
            {                                                    
            //    new Guid("{41E34648-6C9D-44D0-8CA5-941DB051B163}"),
                new Guid("{78D008DF-F7E8-4F99-92A1-42E7AE6E34C3}"),//кемин
                new Guid("{AA466C30-47A6-4252-989D-F44C91A120A5}"),//чуй
                new Guid("{CAE58A37-FBE8-49AA-8E09-B167F871A5E8}"),//город токмок
                new Guid("{BEC45992-2D1C-4488-A260-FC7C5511F619}"),//ыссык-ата
                new Guid("{822E0311-F790-474D-BC97-E67CE8D14009}"),//аламудун
                new Guid("{C139D4DE-9F64-46D2-A908-1C477B78ECCD}"),//сокулук
                new Guid("{CA59DB1A-4AB2-4CB2-90C6-BF323E82E44A}"),//москва
                new Guid("{6612E6F7-BA2B-4E33-ADB9-FA0B14705E27}"),//жайыл
                new Guid("{72075FFB-371D-4C7B-8A5A-993E1C51CBFF}"),//панфилов
            };
            private static List<Guid> talasList = new List<Guid>//Таласская область
            {                                   
            //    new Guid("{BC710978-8C0B-4063-8BA9-A12F75DE4829}"),
                new Guid("{02992EB8-E107-408E-954E-E322A73B433A}"),//г талас
                new Guid("{1E5868C3-B522-4DBF-BC8F-02C75A899C0F}"),//талас рн
                new Guid("{C285A1F2-2114-4752-A626-A235C6D6F2B1}"),//бакай-ата
                new Guid("{17A6CA38-8B7B-4F5C-973B-18F31646CC03}"),//кара-буура
                new Guid("{376FF578-372D-43A4-818E-31BC64327BEF}"),//манас
            };
            private static List<Guid> issyk_kulList = new List<Guid>//Иссык-Кульская область
            {
            //    new Guid("{AD009808-FBAC-43CA-8671-5E790C213497}"),
                new Guid("{43842F64-7BB7-45EC-930B-54AD19186382}"),//каракол
                new Guid("{2FEA2DF5-24F6-4E53-BF6C-22604AC014C1}"),//балыкчы
                new Guid("{416366EE-580B-4E57-8641-0D90A2F5AB73}"),//иссык-куль
                new Guid("{5B438995-2070-46A8-9377-E922F0D64E4F}"),//тюп
                new Guid("{AAD91EAB-297F-40C8-B592-68573495EEC0}"),//жети-огуз
                new Guid("{55F9FFBF-B789-4002-BD64-B04163806897}"),//тон
                new Guid("{D2EDB83E-7EE5-4B5A-82A6-E9E49DD1E3B3}"),//ак-суу
            };
            private static List<Guid> narynList = new List<Guid>//Нарынская область
            {
            //    new Guid("{CF4CA271-9202-4A5A-AE49-EB6237C33982}"),
                new Guid("{CB788BA9-8F94-4317-A655-B4F5527F3A4B}"),//акталаа
                new Guid("{4C60A2A2-73B4-4EA4-A6D0-E2B41370C7FC}"),//ат-баши
                new Guid("{52568E10-0210-4B09-B106-9E14520E26F1}"),//жумгал
                new Guid("{B631F3B0-1656-49C5-9152-0252F304D29B}"),//кочкор
                new Guid("{A6B9B397-81CF-43ED-A17B-6A13C110A678}"),//нарын район
                new Guid("{0A409F28-4A73-4CED-B368-10C6BE53419F}"),//нарын г
            };
            private static List<Guid> batkenList = new List<Guid>//Баткенская область
            {
            //    new Guid("{4E98AD82-8ED4-4727-8E24-678921AB534C}"),
                new Guid("{0BCED873-4950-449A-98E6-69C44F64D70C}"),//баткен рн
                new Guid("{20745158-EAE3-434C-BDF5-5F893C8963ED}"),//баткен г
                new Guid("{E062B2A3-42C4-4619-B086-705E6C5367D3}"),//кадамжай
                new Guid("{F6CECF6A-2D5D-44C1-BC2F-29A836965531}"),//лейлек
                new Guid("{DDE31E57-4454-44EE-AFA9-80227E3C8620}"),//кызыл-кия
                new Guid("{E883C8F3-5E3C-4086-A078-91FD22E6550A}"),//сулюкта
            };
            private static List<Guid> oshList = new List<Guid>//Ошская область
            {
            //    new Guid("{E01E9890-2A41-454E-9EC1-3D4AAF50ED62}"),
                new Guid("{C3C63817-FFB3-4905-BC70-8C3A3DBB67DC}"),//араван
                new Guid("{2A0481C0-FB12-4048-9153-CB7AE997A26C}"),//каракулджа
                new Guid("{3A305A9A-D30E-4C38-9E41-09DF993A658E}"),//кара-суу
                new Guid("{E0AF1DF7-AA64-45DD-873E-510CD413AD35}"),//ноокат
                new Guid("{B0B7490F-E48D-4DC7-9548-0505D73C858F}"),//узген рн
                new Guid("{2F8C280E-6C97-41D5-B24F-7D0DF4FF4C0E}"),//узген г
                new Guid("{5903E184-79E4-4630-AD91-FD488B84B832}"),//алай
                new Guid("{8A7D0C1C-A4A7-4582-8D65-8023CDB273E1}")//чон-алай
            };
            private static Guid osh = new Guid("{A99E469A-E8D4-4139-B89A-CE4AF6AA0733}");
            private static List<Guid> jalal_abad = new List<Guid>//Джалал-Абадская область
            {
            //    new Guid("{3ED50EFB-3E20-407C-93CB-9D8E0EF15B1B}"),
                new Guid("{D319DC07-E7F7-4997-AFCD-17CEAD707B7F}"),//г джалалабад
                new Guid("{8CAC9C70-D770-4AAB-8B0B-E1C432F6985A}"),//кок-жангак
                new Guid("{EFA2A70A-D10E-4941-B1F8-AE5218E5AD29}"),//майлысуу
                new Guid("{8E3CA117-0956-4F67-A8EF-B8222DC0A21E}"),//ташкумыр
                new Guid("{CB86E66E-8A41-4D92-9FD6-9BCF7E61F543}"),//каракуль
                new Guid("{9375DF49-1B41-4EFD-BB79-0FC2A86ABED6}"),//сузак
                new Guid("{A23770C6-2843-446E-8382-8C50AFD699E5}"),//базар-коргон
                new Guid("{A3E2A017-96F2-47C6-AF09-6FA4287E6953}"),//ноокен
                new Guid("{A5B70AEE-4E8B-4194-864D-F58E5BAB76E7}"),//аксы
                new Guid("{915D0BEB-53C1-440F-9CBA-00612369FEFC}"),//ала-бука
                new Guid("{5026031D-2BC9-4A24-8DB5-30C7DD992352}"),//чаткал
                new Guid("{F9D53DFB-0E18-44E6-A905-8B67F5751D37}"),//токтогул рн
                new Guid("{BF0A92AD-140E-4582-AB9F-DB304890C35A}")//тогуз-торо
            };
            public class TempItem
            {
                public Guid CategoryId { get; set; }
                public Guid OrgId { get; set; }
                public int AppCount { get; set; }
            }
            public class ReportItem
            {
                public int RowNo { get; set; }
                public string CategoryName { get; set; }
                public decimal Price { get; set; }
                public int Total { get; set; }
                public int c_Bishkek { get; set; }
                public int r_Chui { get; set; }
                public int r_Talas { get; set; }
                public int r_IK { get; set; }
                public int r_Naryn { get; set; }
                public int r_Batken { get; set; }
                public int r_Osh { get; set; }
                public int c_Osh { get; set; }
                public int r_JA { get; set; }
            }
        }

        public static class FC_1003
        {
            private static readonly Guid privilege46PaymentId = new Guid("{7BEFD6DA-042C-4A77-90F3-A4424033E4DD}");

            // Document Defs Id 1e750c67-2ddf-488e-a4c4-d94547433067
            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");// "{04D25808-6DE9-42F5-8855-6F68A94A224C}"
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{AD72B200-60BA-412B-ADBF-F32D3BE2D40C}");
            private static readonly Guid reportItemDefId = new Guid("{F3FCDA28-FEBB-43A0-AD73-848BB175089E}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");
            // States
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid canceledStateId = new Guid("{C65E63CC-54E9-424E-AFDC-5A8DB1CB04A3}"); // Аннулирован
            private static readonly Guid onApprovingStateId = new Guid("{5CD9E88D-671E-4A44-AD92-9F74DA3B47F7}"); //На утверждении
            private static readonly Guid onRegisteringStateId = new Guid("{A9CD37C4-A718-4DE1-9E95-EC8EC280C8D4}"); // На регистрации
            private static readonly Guid refusedStateId = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}"); // Отказан

            private static readonly Guid lifetimeGrantPayment = new Guid("{3DB68E47-2451-43E0-AB98-9A59C8B52686}");
            private static readonly Guid singleCashBenefit = new Guid("{8FCF2658-55D1-404E-BF87-3D096769FB64}");
            private static readonly Guid pecuniaryCompensationInsteadOfBenefit = new Guid("{7BEFD6DA-042C-4A77-90F3-A4424033E4DD}");
            private static readonly Guid singleGrantTo9May = new Guid("{D450DE1B-ECD9-4286-A46A-59371E437C39}");
            private static readonly Guid singleAdditionalGrantTo9May = new Guid("{D450DE1B-ECD9-4286-A46A-59371E437C39}");
            private static readonly Guid sumSinglePaymentToPerMan = new Guid("{62D22220-F570-4C24-9859-6B3E67B26C5A}");

            private static Guid category1 = new Guid("{2E662509-144D-47CA-9F2B-087BB6F4AE91}");//Участник ликвидации последствий аварии на ЧАЭЗ 1989 г.
            private static Guid category2 = new Guid("{DF92F99F-BA8D-415C-8236-F54206AF0F73}");//Участник ликвидации последствий аварии на ЧАЭЗ 1988 г.
            private static Guid category3 = new Guid("{8691D5FB-7611-4AAA-AD84-60D2AADF50BE}");//Участник ликвидации последствий аварии на ЧАЭЗ 1986-87 гг.
            private static Guid category4 = new Guid("{63DFD63A-2868-411F-A52F-039E37B1D39F}");//Участник ВОВ, награжденный орденами славы 3-х степеней
            private static Guid category5 = new Guid("{677A9F5A-79F3-43F1-948E-AE10F6007677}");//Участник ВОВ
            private static Guid category6 = new Guid("{7EE64986-5A47-49D0-A288-88A537E6D0A0}");//Участник боевых действий на территории других государств
            private static Guid category7 = new Guid("{145C4F38-9BCF-44B3-95D0-EA1B8D1B869B}");//Участник блокады г.Ленинграда
            private static Guid category8 = new Guid("{8C21641E-1AC2-438A-ABDB-D6D17D195BB5}");//Семьям погибших военнослужащих при исполнении обязанностей военной службы
            private static Guid category9 = new Guid("{ECD8E6DF-3C3E-427D-BDC1-58E132A8B03D}");//Семья, потерявшая кормильца - участника ЧАЭС
            private static Guid category10 = new Guid("{93B5323F-E16B-4A76-AEB6-7CE166B0471B}");//Семья погибшего/пропавшего без вести в ВОВ военнослужащего
            private static Guid category11 = new Guid("{B038B840-021C-436E-B34D-4EB0E90DB767}");//Семьи сотрудников МВД, погибших при исполнении сл. обяз. или сл.долга, умерших после увольн. вслед-е ран./трав./забол., получ. в пер.прохожд.службы
            private static Guid category12 = new Guid("{C5728DEE-CE74-41C2-A0CF-B1182615D6B3}");//Реабилитированный, пострадавший в результате репрессий
            private static Guid category13 = new Guid("{24D5389C-9102-4369-A978-E2EF7DFC36C8}");//Почетный донор
            private static Guid category14 = new Guid("{C8B1465C-F377-4D9B-B23C-04FFC6836597}");//Несовершеннолетний узник концлагерей, гетто
            private static Guid category15 = new Guid("{FA1787EF-A156-400E-8CA3-436203D3C322}");//ЛОВЗ Сов.Армии 3 гр. (во время несения службы)
            private static Guid category16 = new Guid("{22CB5768-069B-4E18-9F4C-D3F308AF4E3F}");//ЛОВЗ Сов.Армии 2 гр. (во время несения службы)
            private static Guid category17 = new Guid("{1EA3FA92-E895-4A9D-AD5F-72FE1EE3D5BF}");//ЛОВЗ Сов.Армии 1 гр. (во время несения службы)
            private static Guid category18 = new Guid("{CBD190B1-44AD-4E6B-8B79-34BF60619CF5}");//ЛОВЗ по слуху и зрению до 18 лет
            private static Guid category19 = new Guid("{DB4E8C78-A17F-44FD-890D-A67F1EA2B457}");//ЛОВЗ 3 гр. по слуху
            private static Guid category21 = new Guid("{18A54BA7-0330-40B3-AE9E-67D049004AB0}");//ЛОВЗ 2 гр. по слуху
            private static Guid category23 = new Guid("{C7B397DF-8325-4F0D-A94C-F8BD5E8152E4}");//ЛОВЗ 1 гр. по слуху
            private static Guid category20 = new Guid("{8B787FCA-62EE-4261-9094-1FFAC1BF4C02}");//ЛОВЗ 3 гр. по зрению
            private static Guid category22 = new Guid("{947E64F0-18EC-4A6B-80C0-FCECFE7C67C2}");//ЛОВЗ 2 гр. по зрению
            private static Guid category24 = new Guid("{5EE97C99-B2A1-4759-9A28-876B08FE7BA8}");//ЛОВЗ 1 гр. по зрению
            private static Guid category25 = new Guid("{A4B6BC13-479B-468F-B4EB-D661417B1127}");//Лица, награжденные за работу/службу в тылу в годы ВОВ с группой инв.
            private static Guid category26 = new Guid("{891911A4-C7C9-4E8D-919B-6AE2B7DE1AB7}");//Лица, награжденные за работу/службу в тылу в годы ВОВ без группы инв-ти
            private static Guid category27 = new Guid("{76668EA1-47E2-4F09-AD73-4AF24BF0D55C}");//Лица заболевшие лучевой болезнью (ЧАЭС)
            private static Guid category28 = new Guid("{E14662C0-719D-4B26-886F-55C8BD43A11E}");//Инвалид-реабилитиров., пострадавший в результате репрессий
            private static Guid category29 = new Guid("{BFE0BD8C-032D-4DF2-ACC1-60F2385925E9}");//Инвалид Советской армии при исполнении служеб. Обязанности
            private static Guid category30 = new Guid("{62960950-7E8F-420F-BCE6-D7F92CA4EA93}");//Инвалид ВОВ 3 группы
            private static Guid category31 = new Guid("{626357F4-5133-4426-A712-2E785E556F6E}");//Инвалид ВОВ 2 группы
            private static Guid category32 = new Guid("{32B76FDF-145B-492C-A850-BE903B7AB6CA}");//Инвалид ВОВ 1 группы
            private static Guid category33 = new Guid("{7CC482B7-A2FD-4B2E-8BFE-9CF0B96D8E05}");//Инв.3 гр.-участник ликвидации последствий аварии на ЧАЭC
            private static Guid category35 = new Guid("{69E2D977-7C5A-4315-BF6A-6E790D19D7EC}");//Инв.2 гр.-участник ликвидации последствий аварии на ЧАЭC
            private static Guid category38 = new Guid("{0A8D29A0-4806-422B-89EF-8F11AE835098}");//Инв.1 гр. - участник ликвидации последствий аварии на ЧАЭC
            private static Guid category34 = new Guid("{8828EAF5-5884-4BB0-B097-3D65496E4168}");//Инв.3 гр.-участник боевых действий на террит.др.государств
            private static Guid category36 = new Guid("{EB091676-B785-42AC-A330-44DFFBFDE1F6}");//Инв.2 гр.-участник боевых действий на террит.др.государств
            private static Guid category37 = new Guid("{3F0DD390-EFF5-42BA-A6D4-BB600F61F793}");//Инв.1 гр.-участник боевых действий на террит.др.государств
            private static Guid category39 = new Guid("{DCAC340F-4132-4514-B90C-581B2B24E37D}");//Дети участника ЧАЭС в возрасте до 18 лет
            private static Guid category40 = new Guid("{2222FD98-B885-4DC0-A0D4-271600AF281A}");//Дети ВИЧ-инфиц. или больные СПИДом
            private static Guid category41 = new Guid("{B0004A56-4831-4CB8-B12B-C39280493A0B}");//Гражданин, мобилизованный в трудовую армию в годы ВОВ
            private static Guid category42 = new Guid("{A1FD4E39-07A0-4117-A99F-1CBFB9BF9E97}");//Герой СССР, герой КР, награжденный за боевые заслуги
            private static Guid category43 = new Guid("{95EC8C3C-0006-45E6-9A24-0FBA212117AA}");//Герой соц. труда, кавалер ордена трудовой славы 3-х степеней
            private static Guid category44 = new Guid("{92F076ED-8558-416F-B28C-AD80017C8D56}");//Вдова(ец) участ. Блокады Ленинграда
            private static Guid category45 = new Guid("{5F257499-9BF5-4850-AC3B-02264CDB62C8}");//Вдова(ец) УОВ
            private static Guid category46 = new Guid("{93933D4F-DE39-4C43-839F-6BE09DF1C9C5}");//Вдова(ец) умершего ИОВ
            private static Guid category47 = new Guid("{93b5323f-e16b-4a76-aeb6-7ce166b0471b}");//Семья погибшего/пропавшего без вести в ВОВ военнослужащего

            private static List<object> categoryTypes = new List<object>
            {
                category30,category31,category32,category5,category14,category7,category47,category25,category26,category41
            };

            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {
                var items = new List<ReportItem>();
                var fd = new DateTime(year, month, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var qb = new QueryBuilder(assignmentDefId, context.UserId);
                qb.Where("PaymentType").In(
                    new object[]
                {
                    lifetimeGrantPayment,
                    singleCashBenefit,
                    pecuniaryCompensationInsteadOfBenefit,
                    singleGrantTo9May,
                    singleAdditionalGrantTo9May,
                    sumSinglePaymentToPerMan
                }).And("EffectiveDate").Lt(ld).And("ExpiryDate").Gt(fd);

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("&Id", SqlQuerySummaryFunction.Count);
                query.AddAttribute("Amount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute("PaymentType");
                query.AddAttribute("Category");
                query.AddGroupAttribute("PaymentType");
                query.AddGroupAttribute("Category");


                var tempList = new List<TempItem>();

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        var count = reader.IsDbNull(0) ? 0 : reader.GetInt32(0);
                        var sum = reader.IsDbNull(1) ? 0m : reader.GetDecimal(1);
                        var paymentId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                        var categoryId = reader.IsDbNull(3) ? Guid.Empty : reader.GetGuid(3);

                        tempList.Add(new TempItem
                        {
                            CategoryId = categoryId,
                            PaymentId = paymentId,
                            Count = count,
                            Sum = sum
                        });
                    }
                }

                CategoryItem categoryItem = new CategoryItem();
                List<ReportItem> reportItemList = new List<ReportItem>();
                List<object> catList = new List<object>();
                catList.Add(category30);
                catList.Add(category31);
                catList.Add(category32);
                var subItems1 = tempList.Where(x => catList.Contains(x.CategoryId));
                categoryItem.category3032 = SetCategoryItem(catList, subItems1, "Инвалид ВОВ", 1);
                reportItemList.Add(categoryItem.category3032);
                catList.Clear();
                catList.Add(category5);
                var subItems2 = tempList.Where(x => catList.Contains(x.CategoryId));
                categoryItem.category5 = SetCategoryItem(catList, subItems2, "Участники ВОВ", 2);
                reportItemList.Add(categoryItem.category5);
                catList.Clear();
                catList.Add(category14);
                var subItems3 = tempList.Where(x => catList.Contains(x.CategoryId));
                categoryItem.category14 = SetCategoryItem(catList, subItems3, "Несовершеннолетние узники концлагерей", 3);
                reportItemList.Add(categoryItem.category14);
                catList.Clear();
                catList.Add(category7);
                var subItems4 = tempList.Where(x => catList.Contains(x.CategoryId));
                categoryItem.category7 = SetCategoryItem(catList, subItems4, "Блокадники  Ленинграда", 4);
                reportItemList.Add(categoryItem.category7);
                categoryItem.TotalFirst = SetTotal(reportItemList);

                reportItemList.Clear();
                catList.Clear();
                catList.Add(category47);
                var subItems5 = tempList.Where(x => catList.Contains(x.CategoryId));
                categoryItem.category47 = SetCategoryItem(catList, subItems5, "Семьи погибших  воинов в ВОВ", 6);
                reportItemList.Add(categoryItem.category47);
                catList.Clear();
                catList.Add(category25);
                var subItems6 = tempList.Where(x => catList.Contains(x.CategoryId));
                categoryItem.category25 = SetCategoryItem(catList, subItems6, "Труженики тыла с группой инвалидности", 7);
                reportItemList.Add(categoryItem.category25);
                catList.Clear();
                catList.Add(category26);
                var subItems7 = tempList.Where(x => catList.Contains(x.CategoryId));
                categoryItem.category26 = SetCategoryItem(catList, subItems7, "Труженики тыла (без инвалидности)", 8);
                reportItemList.Add(categoryItem.category26);
                catList.Clear();
                catList.Add(category41);
                var subItems8 = tempList.Where(x => catList.Contains(x.CategoryId));
                categoryItem.category41 = SetCategoryItem(catList, subItems8, "Труд армейцы", 9);
                reportItemList.Add(categoryItem.category41);
                categoryItem.TotalSecond = SetTotal(reportItemList);

                return ObjectToList(categoryItem);
            }

            private static List<ReportItem> ObjectToList(CategoryItem categoryItem)
            {
                List<ReportItem> reportItemList = new List<ReportItem>();
                Type type = categoryItem.GetType();
                PropertyInfo[] properties = type.GetProperties();

                foreach (PropertyInfo property in properties)
                {
                    reportItemList.Add((ReportItem)property.GetValue(categoryItem, null));
                }
                return reportItemList;
            }


            private static ReportItem SetTotal(List<ReportItem> reportItemList)
            {
                ReportItem reportItem = new ReportItem();
                //reportItem.RowNo = 5;
                reportItem.CategoryName = "Итого";
                foreach (ReportItem subItem in reportItemList)
                {
                    reportItem.Count += subItem.Count;
                    reportItem.lifetimeGrantPayment += subItem.lifetimeGrantPayment;
                    reportItem.singleCashBenefit += subItem.singleCashBenefit;
                    reportItem.lifetimeGrantPayment += subItem.pecuniaryCompensationInsteadOfBenefit;
                    reportItem.singleGrantTo9May += subItem.singleGrantTo9May;
                    reportItem.singleAdditionalGrantTo9May += subItem.singleAdditionalGrantTo9May;
                    reportItem.sumSinglePaymentToPerMan += subItem.sumSinglePaymentToPerMan;
                }
                return reportItem;
            }
            private static ReportItem SetCategoryItem(List<object> catList, IEnumerable<TempItem> subItems, string categoryName, int rowNo)
            {
                ReportItem reportItem = new ReportItem();
                reportItem.CategoryName = categoryName;
                reportItem.RowNo = rowNo;
                reportItem.Count = subItems.Count();
                reportItem.lifetimeGrantPayment = subItems.Where(x => x.PaymentId == lifetimeGrantPayment).Sum(x => x.Sum);
                reportItem.singleCashBenefit = subItems.Where(x => x.PaymentId == singleCashBenefit).Sum(x => x.Sum);
                reportItem.pecuniaryCompensationInsteadOfBenefit = subItems.Where(x => x.PaymentId == pecuniaryCompensationInsteadOfBenefit).Sum(x => x.Sum);
                reportItem.singleGrantTo9May = subItems.Where(x => x.PaymentId == singleGrantTo9May).Sum(x => x.Sum);
                reportItem.singleAdditionalGrantTo9May = subItems.Where(x => x.PaymentId == singleAdditionalGrantTo9May).Sum(x => x.Sum);
                reportItem.sumSinglePaymentToPerMan = subItems.Where(x => x.PaymentId == sumSinglePaymentToPerMan).Sum(x => x.Sum);
                return reportItem;
            }


            private static ReportItem GetReportItem(List<ReportItem> items, string categoryName)
            {
                var item = items.FirstOrDefault(x => x.CategoryName == categoryName);
                if (item != null) return item;
                item = new ReportItem
                {
                    CategoryName = categoryName,
                    RowNo = items.Count > 0 ? (items.Max(x => x.RowNo) + 1) : 1
                };
                items.Add(item);
                return item;
            }

            public class CategoryItem
            {
                public ReportItem category3032 { get; set; }
                public ReportItem category5 { get; set; }
                public ReportItem category14 { get; set; }
                public ReportItem category7 { get; set; }
                public ReportItem TotalFirst { get; set; }

                public ReportItem category47 { get; set; }
                public ReportItem category25 { get; set; }
                public ReportItem category26 { get; set; }
                public ReportItem category41 { get; set; }
                public ReportItem TotalSecond { get; set; }
            }



            public class TempItem
            {
                public Guid PaymentId { get; set; }
                public Guid CategoryId { get; set; }
                public int Count { get; set; }
                public decimal Sum { get; set; }
            }

            public class ReportItem
            {
                public int RowNo { get; set; }
                public string CategoryName { get; set; }
                public int Count { get; set; }
                public decimal lifetimeGrantPayment { get; set; }
                public decimal singleCashBenefit { get; set; }
                public decimal pecuniaryCompensationInsteadOfBenefit { get; set; }
                public decimal singleGrantTo9May { get; set; }
                public decimal singleAdditionalGrantTo9May { get; set; }
                public decimal sumSinglePaymentToPerMan { get; set; }
            }

        }

        public static class FC_1004
        {
            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");// "{04D25808-6DE9-42F5-8855-6F68A94A224C}"
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{AD72B200-60BA-412B-ADBF-F32D3BE2D40C}");
            private static readonly Guid reportItemDefId = new Guid("{F3FCDA28-FEBB-43A0-AD73-848BB175089E}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");

            private static Guid category30 = new Guid("{62960950-7E8F-420F-BCE6-D7F92CA4EA93}");//Инвалид ВОВ 3 группы
            private static Guid category31 = new Guid("{626357F4-5133-4426-A712-2E785E556F6E}");//Инвалид ВОВ 2 группы
            private static Guid category32 = new Guid("{32B76FDF-145B-492C-A850-BE903B7AB6CA}");//Инвалид ВОВ 1 группы

            private static Guid category5 = new Guid("{677A9F5A-79F3-43F1-948E-AE10F6007677}");//Участник ВОВ

            private static Guid category42 = new Guid("{A1FD4E39-07A0-4117-A99F-1CBFB9BF9E97}");//Герой СССР, герой КР, награжденный за боевые заслуги

            private static Guid category14 = new Guid("{C8B1465C-F377-4D9B-B23C-04FFC6836597}");//Несовершеннолетний узник концлагерей, гетто

            private static Guid category7 = new Guid("{145C4F38-9BCF-44B3-95D0-EA1B8D1B869B}");//Участник блокады г.Ленинграда

            private static Guid category15 = new Guid("{FA1787EF-A156-400E-8CA3-436203D3C322}");//ЛОВЗ Сов.Армии 3 гр. (во время несения службы)
            private static Guid category16 = new Guid("{22CB5768-069B-4E18-9F4C-D3F308AF4E3F}");//ЛОВЗ Сов.Армии 2 гр. (во время несения службы)
            private static Guid category17 = new Guid("{1EA3FA92-E895-4A9D-AD5F-72FE1EE3D5BF}");//ЛОВЗ Сов.Армии 1 гр. (во время несения службы)
            private static Guid category29 = new Guid("{BFE0BD8C-032D-4DF2-ACC1-60F2385925E9}");//Инвалид Советской армии при исполнении служеб. Обязанности

            private static Guid category34 = new Guid("{8828EAF5-5884-4BB0-B097-3D65496E4168}");//Инв.3 гр.-участник боевых действий на террит.др.государств
            private static Guid category36 = new Guid("{EB091676-B785-42AC-A330-44DFFBFDE1F6}");//Инв.2 гр.-участник боевых действий на террит.др.государств
            private static Guid category37 = new Guid("{3F0DD390-EFF5-42BA-A6D4-BB600F61F793}");//Инв.1 гр.-участник боевых действий на террит.др.государств

            private static Guid category6 = new Guid("{7EE64986-5A47-49D0-A288-88A537E6D0A0}");//Участник боевых действий на территории других государств

            private static Guid category33 = new Guid("{7CC482B7-A2FD-4B2E-8BFE-9CF0B96D8E05}");//Инв.3 гр.-участник ликвидации последствий аварии на ЧАЭC
            private static Guid category35 = new Guid("{69E2D977-7C5A-4315-BF6A-6E790D19D7EC}");//Инв.2 гр.-участник ликвидации последствий аварии на ЧАЭC
            private static Guid category38 = new Guid("{0A8D29A0-4806-422B-89EF-8F11AE835098}");//Инв.1 гр. - участник ликвидации последствий аварии на ЧАЭC

            private static Guid category3 = new Guid("{8691D5FB-7611-4AAA-AD84-60D2AADF50BE}");//Участник ликвидации последствий аварии на ЧАЭЗ 1986-87 гг.

            private static Guid category25 = new Guid("{A4B6BC13-479B-468F-B4EB-D661417B1127}");//Лица, награжденные за работу/службу в тылу в годы ВОВ с группой инв.

            private static Guid category41 = new Guid("{B0004A56-4831-4CB8-B12B-C39280493A0B}");//Гражданин, мобилизованный в трудовую армию в годы ВОВ

            private static Guid category12 = new Guid("{C5728DEE-CE74-41C2-A0CF-B1182615D6B3}");//Реабилитированный, пострадавший в результате репрессий

            private static Guid category1 = new Guid("{2E662509-144D-47CA-9F2B-087BB6F4AE91}");//Участник ликвидации последствий аварии на ЧАЭЗ 1989 г.
            private static Guid category2 = new Guid("{DF92F99F-BA8D-415C-8236-F54206AF0F73}");//Участник ликвидации последствий аварии на ЧАЭЗ 1988 г.

            private static Guid category9 = new Guid("{ECD8E6DF-3C3E-427D-BDC1-58E132A8B03D}");//Семья, потерявшая кормильца - участника ЧАЭС

            private static Guid category39 = new Guid("{DCAC340F-4132-4514-B90C-581B2B24E37D}");//Дети участника ЧАЭС в возрасте до 18 лет

            private static Guid category26 = new Guid("{891911A4-C7C9-4E8D-919B-6AE2B7DE1AB7}");//Лица, награжденные за работу/службу в тылу в годы ВОВ без группы инв-ти

            private static Guid category8 = new Guid("{8C21641E-1AC2-438A-ABDB-D6D17D195BB5}");//Семьям погибших военнослужащих при исполнении обязанностей военной службы

            private static Guid category10 = new Guid("{93B5323F-E16B-4A76-AEB6-7CE166B0471B}");//Семья погибшего/пропавшего без вести в ВОВ военнослужащего

            private static Guid category11 = new Guid("{B038B840-021C-436E-B34D-4EB0E90DB767}");//Семьи сотрудников МВД, погибших при исполнении сл. обяз. или сл.долга, умерших после увольн. вслед-е ран./трав./забол., получ. в пер.прохожд.службы

            private static Guid category45 = new Guid("{5F257499-9BF5-4850-AC3B-02264CDB62C8}");//Вдова(ец) УОВ

            private static Guid category46 = new Guid("{93933D4F-DE39-4C43-839F-6BE09DF1C9C5}");//Вдова(ец) умершего ИОВ

            private static Guid category44 = new Guid("{92F076ED-8558-416F-B28C-AD80017C8D56}");//Вдова(ец) участ. Блокады Ленинграда

            private static Guid category13 = new Guid("{24D5389C-9102-4369-A978-E2EF7DFC36C8}");//Почетный донор

            private static Guid category18 = new Guid("{CBD190B1-44AD-4E6B-8B79-34BF60619CF5}");//ЛОВЗ по слуху и зрению до 18 лет
            private static Guid category19 = new Guid("{DB4E8C78-A17F-44FD-890D-A67F1EA2B457}");//ЛОВЗ 3 гр. по слуху
            private static Guid category21 = new Guid("{18A54BA7-0330-40B3-AE9E-67D049004AB0}");//ЛОВЗ 2 гр. по слуху
            private static Guid category23 = new Guid("{C7B397DF-8325-4F0D-A94C-F8BD5E8152E4}");//ЛОВЗ 1 гр. по слуху
            private static Guid category20 = new Guid("{8B787FCA-62EE-4261-9094-1FFAC1BF4C02}");//ЛОВЗ 3 гр. по зрению
            private static Guid category22 = new Guid("{947E64F0-18EC-4A6B-80C0-FCECFE7C67C2}");//ЛОВЗ 2 гр. по зрению
            private static Guid category24 = new Guid("{5EE97C99-B2A1-4759-9A28-876B08FE7BA8}");//ЛОВЗ 1 гр. по зрению


            private static List<Guid> categoryGroup1 = new List<Guid>
            {
                category30,category31,category32
            };

            private static List<Guid> categoryGroup2 = new List<Guid>
            {
                category5
            };

            private static List<Guid> categoryGroup3 = new List<Guid>
            {
                category42
            };

            private static List<Guid> categoryGroup4 = new List<Guid>
            {
                category14
            };

            private static List<Guid> categoryGroup5 = new List<Guid>
            {
                category7
            };

            private static List<Guid> categoryGroup6 = new List<Guid>
            {
                category15, category16,category17,category29
            };

            private static List<Guid> categoryGroup7 = new List<Guid>
            {
                category34, category36,category37
            };

            private static List<Guid> categoryGroup8 = new List<Guid>
            {
                category6
            };

            private static List<Guid> categoryGroup9 = new List<Guid>
            {
                category33,category35,category38
            };

            private static List<Guid> categoryGroup10 = new List<Guid>
            {
                category3
            };

            private static List<Guid> categoryGroup11 = new List<Guid>
            {
                category25
            };

            private static List<Guid> categoryGroup12 = new List<Guid>
            {
                category41
            };

            private static List<Guid> categoryGroup13 = new List<Guid>
            {
                category12
            };

            private static List<Guid> categoryGroup14 = new List<Guid>
            {
                category1,category2
            };

            private static List<Guid> categoryGroup15 = new List<Guid>
            {
                category9
            };

            private static List<Guid> categoryGroup16 = new List<Guid>
            {
                category39
            };

            private static List<Guid> categoryGroup17 = new List<Guid>
            {
                category26
            };

            private static List<Guid> categoryGroup18 = new List<Guid>
            {
                category8
            };

            private static List<Guid> categoryGroup19 = new List<Guid>
            {
                category10
            };

            private static List<Guid> categoryGroup20 = new List<Guid>
            {
                category11
            };

            private static List<Guid> categoryGroup21 = new List<Guid>
            {
                category45
            };

            private static List<Guid> categoryGroup22 = new List<Guid>
            {
                category46
            };

            private static List<Guid> categoryGroup23 = new List<Guid>
            {
                category44
            };

            private static List<Guid> categoryGroup24 = new List<Guid>
            {
                category13
            };

            private static List<Guid> categoryGroup25 = new List<Guid>
            {
                category18,category19,category21,category23,category20,category22,category24
            };

            public static List<YearReportItem> Execute(WorkflowContext context, int year, int month)
            {
                ReportItem reportItem = new ReportItem();
                reportItem = Build(context, year, "year_0", reportItem);
                reportItem = Build(context, year - 1, "year_1", reportItem);
                reportItem = Build(context, year - 2, "year_2", reportItem);
                return convertObjectToList(reportItem);
            }

            private static ReportItem Build(WorkflowContext context, int year, string yearPropertyName, ReportItem reportItem)
            {
                var fd = new DateTime(year, 1, 1);
                var ld = new DateTime(year, 12, DateTime.DaysInMonth(year, 12));
                var qb = new QueryBuilder(assignmentDefId, context.UserId);
                qb.Where("EffectiveDate").Lt(ld).And("ExpiryDate").Gt(fd);

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("&Id", SqlQuerySummaryFunction.Count);
                query.AddAttribute("Amount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute("Category");
                query.AddGroupAttribute("Category");

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        var count = reader.IsDbNull(0) ? 0 : reader.GetInt32(0);
                        var sum = reader.IsDbNull(1) ? 0m : reader.GetDecimal(1);
                        var categoryId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                        if (categoryId == Guid.Empty) continue;
                        reportItem = calc(reportItem, categoryId, sum, yearPropertyName);
                    }
                }
                return reportItem;
            }

            private static List<YearReportItem> convertObjectToList(ReportItem item)
            {
                List<YearReportItem> reportItemList = new List<YearReportItem>();
                Type type = item.GetType();
                PropertyInfo[] properties = type.GetProperties();
                foreach (PropertyInfo property in properties)
                {
                    reportItemList.Add((YearReportItem)property.GetValue(item, null));
                }
                return reportItemList;
            }

            private static ReportItem setValueByProperty(ReportItem item, string reportItemPropertyName, decimal value, string yearPropertyName)
            {
                if (!string.IsNullOrEmpty(reportItemPropertyName))
                {
                    PropertyInfo propertyInfo = item.GetType().GetProperty(reportItemPropertyName);
                    YearReportItem prevValueReportItem = (YearReportItem)propertyInfo.GetValue(item, null);
                    PropertyInfo propertyInfoYear = prevValueReportItem.GetType().GetProperty(yearPropertyName);
                    decimal prevValueYear = (decimal)propertyInfoYear.GetValue(prevValueReportItem, null);
                    propertyInfoYear.SetValue(prevValueReportItem, prevValueYear + value, null);
                    propertyInfo.SetValue(item, prevValueReportItem, null);
                }
                return item;
            }

            private static ReportItem calc(ReportItem reportItem, Guid categoryId, decimal sum, string yearPropertyName)
            {
                Type type = typeof(FC_1004);
                foreach (var pi in type.GetFields(System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic))
                {
                    if (isList(pi.FieldType))
                    {
                        var listGuid = (List<Guid>)pi.GetValue(null);
                        if (listGuid.Contains(categoryId))
                            setValueByProperty(reportItem, pi.Name, sum, yearPropertyName);
                    }
                }
                return reportItem;
            }




            private static bool isList(Type type)
            {
                return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>);
            }


            [DataContract]
            public class YearReportItem
            {
                public YearReportItem(string CategoryName)
                {
                    categoryName = CategoryName;
                    year_0 = 0;
                    year_1 = 0;
                    year_2 = 0;
                }
                [DataMember]
                public string categoryName { get; set; }
                [DataMember]
                public decimal year_0 { get; set; }
                [DataMember]
                public decimal year_1 { get; set; }
                [DataMember]
                public decimal year_2 { get; set; }
            }

            public class ReportItem
            {
                public ReportItem()
                {
                    categoryGroup1 = new YearReportItem("Инвалиды ВОВ");
                    categoryGroup2 = new YearReportItem("Участники ВОВ");
                    categoryGroup3 = new YearReportItem("Герои КР, Советского Союза");
                    categoryGroup4 = new YearReportItem("Несовершеннолетние узники концлагерей");
                    categoryGroup5 = new YearReportItem("Блокадники г. Ленинграда");
                    categoryGroup6 = new YearReportItem("Инвалиды СА");
                    categoryGroup7 = new YearReportItem("Инвалиды интернационалисты");
                    categoryGroup8 = new YearReportItem("Участники интернационалисты");
                    categoryGroup9 = new YearReportItem("Инвалиды ЧАЭС");
                    categoryGroup10 = new YearReportItem("Участники ЧАЭС 1986-1987 гг.");
                    categoryGroup11 = new YearReportItem("Труженики тыла с группой инвалидности");
                    categoryGroup12 = new YearReportItem("Труд армейцы");
                    categoryGroup13 = new YearReportItem("Реабилитированные и пострадавшие граждане");
                    categoryGroup14 = new YearReportItem("Участники ЧАЭС 1988-1989 гг.");
                    categoryGroup15 = new YearReportItem("СП участников и умерших инвалидов ЧАЭС");
                    categoryGroup16 = new YearReportItem("Дети до 18 лет участников ЧАЭС");
                    categoryGroup17 = new YearReportItem("Труженики тыла (без инвалидности)");
                    categoryGroup18 = new YearReportItem("Семьи погибших военнослужащих");
                    categoryGroup19 = new YearReportItem("Семьи погибших воинов в ВОВ");
                    categoryGroup20 = new YearReportItem("Семьи погибших сотрудников ОВД");
                    categoryGroup21 = new YearReportItem("Вдовы (вдовцы) инвалидов ВОВ");
                    categoryGroup22 = new YearReportItem("Вдовы (вдовцы) участников ВОВ");
                    categoryGroup23 = new YearReportItem("Вдовы блокадников Ленинграда");
                    categoryGroup24 = new YearReportItem("Почетные доноры");
                    categoryGroup25 = new YearReportItem("ЛОВЗ по зрению и слуху");
                }
                public YearReportItem categoryGroup1 { get; set; }
                public YearReportItem categoryGroup2 { get; set; }
                public YearReportItem categoryGroup3 { get; set; }
                public YearReportItem categoryGroup4 { get; set; }
                public YearReportItem categoryGroup5 { get; set; }
                public YearReportItem categoryGroup6 { get; set; }
                public YearReportItem categoryGroup7 { get; set; }
                public YearReportItem categoryGroup8 { get; set; }
                public YearReportItem categoryGroup9 { get; set; }
                public YearReportItem categoryGroup10 { get; set; }
                public YearReportItem categoryGroup11 { get; set; }
                public YearReportItem categoryGroup12 { get; set; }
                public YearReportItem categoryGroup13 { get; set; }
                public YearReportItem categoryGroup14 { get; set; }
                public YearReportItem categoryGroup15 { get; set; }
                public YearReportItem categoryGroup16 { get; set; }
                public YearReportItem categoryGroup17 { get; set; }
                public YearReportItem categoryGroup18 { get; set; }
                public YearReportItem categoryGroup19 { get; set; }
                public YearReportItem categoryGroup20 { get; set; }
                public YearReportItem categoryGroup21 { get; set; }
                public YearReportItem categoryGroup22 { get; set; }
                public YearReportItem categoryGroup23 { get; set; }
                public YearReportItem categoryGroup24 { get; set; }
                public YearReportItem categoryGroup25 { get; set; }
            }


        }


        public static class FC_1005
        {
            #region Объявление ID
            private static readonly Guid personDefId = new Guid("{6F5B8A06-361E-4559-8A53-9CB480A9B16C}");
            private static readonly Guid privilege46PaymentId = new Guid("{7BEFD6DA-042C-4A77-90F3-A4424033E4DD}");
            // Document Defs Id 1e750c67-2ddf-488e-a4c4-d94547433067
            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");// "{04D25808-6DE9-42F5-8855-6F68A94A224C}"
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{AD72B200-60BA-412B-ADBF-F32D3BE2D40C}");
            private static readonly Guid reportItemDefId = new Guid("{F3FCDA28-FEBB-43A0-AD73-848BB175089E}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");
            // States
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid canceledStateId = new Guid("{C65E63CC-54E9-424E-AFDC-5A8DB1CB04A3}"); // Аннулирован
            private static readonly Guid onApprovingStateId = new Guid("{5CD9E88D-671E-4A44-AD92-9F74DA3B47F7}"); //На утверждении
            private static readonly Guid onRegisteringStateId = new Guid("{A9CD37C4-A718-4DE1-9E95-EC8EC280C8D4}"); // На регистрации
            private static readonly Guid refusedStateId = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}");

            private static Guid category29 = new Guid("{BFE0BD8C-032D-4DF2-ACC1-60F2385925E9}");//Инвалид Советской армии при исполнении служеб. Обязанности
            private static Guid category33 = new Guid("{7CC482B7-A2FD-4B2E-8BFE-9CF0B96D8E05}");//Инв.3 гр.-участник ликвидации последствий аварии на ЧАЭC
            private static Guid category35 = new Guid("{69E2D977-7C5A-4315-BF6A-6E790D19D7EC}");//Инв.2 гр.-участник ликвидации последствий аварии на ЧАЭC
            private static Guid category38 = new Guid("{0A8D29A0-4806-422B-89EF-8F11AE835098}");//Инв.1 гр. - участник ликвидации последствий аварии на ЧАЭC

            private static Guid category18 = new Guid("{CBD190B1-44AD-4E6B-8B79-34BF60619CF5}");//ЛОВЗ по слуху и зрению до 18 лет
            private static Guid category19 = new Guid("{DB4E8C78-A17F-44FD-890D-A67F1EA2B457}");//ЛОВЗ 3 гр. по слуху
            private static Guid category21 = new Guid("{18A54BA7-0330-40B3-AE9E-67D049004AB0}");//ЛОВЗ 2 гр. по слуху
            private static Guid category23 = new Guid("{C7B397DF-8325-4F0D-A94C-F8BD5E8152E4}");//ЛОВЗ 1 гр. по слуху
            private static Guid category20 = new Guid("{8B787FCA-62EE-4261-9094-1FFAC1BF4C02}");//ЛОВЗ 3 гр. по зрению
            private static Guid category22 = new Guid("{947E64F0-18EC-4A6B-80C0-FCECFE7C67C2}");//ЛОВЗ 2 гр. по зрению
            private static Guid category24 = new Guid("{5EE97C99-B2A1-4759-9A28-876B08FE7BA8}");//ЛОВЗ 1 гр. по зрению

            private static Guid category13 = new Guid("{24D5389C-9102-4369-A978-E2EF7DFC36C8}");//Почетный донор

            private static Guid category39 = new Guid("{DCAC340F-4132-4514-B90C-581B2B24E37D}");//Дети участника ЧАЭС в возрасте до 18 лет

            private static Guid category8 = new Guid("{8C21641E-1AC2-438A-ABDB-D6D17D195BB5}");//Семьям погибших военнослужащих при исполнении обязанностей военной службы
            private static Guid category9 = new Guid("{ECD8E6DF-3C3E-427D-BDC1-58E132A8B03D}");//Семья, потерявшая кормильца - участника ЧАЭС

            private static Guid category11 = new Guid("{B038B840-021C-436E-B34D-4EB0E90DB767}");//Семьи сотрудников МВД, погибших при исполнении сл. обяз. или сл.долга, умерших после увольн. вслед-е ран./трав./забол., получ. в пер.прохожд.службы

            public static List<Guid> CategoryType = new List<Guid>
            {
                category29,category33,category35,category38,category19,category21,category23,
                category20,category22,category24, category18,category13,category39,category8,
                category9,category8
            };
            private static List<Guid> categoryGroup1 = new List<Guid>
            {
                category29
            };

            private static List<Guid> categoryGroup2 = new List<Guid>
            {
                category33,category35,category38
            };

            private static List<Guid> categoryGroup3 = new List<Guid>
            {
                category19,category21,category23
            };

            private static List<Guid> categoryGroup4 = new List<Guid>
            {
                category20,category22,category24
            };

            private static List<Guid> categoryGroup5 = new List<Guid>
            {
                category18
            };

            private static List<Guid> categoryGroup6 = new List<Guid>
            {
                category13
            };

            private static List<Guid> categoryGroup7 = new List<Guid>
            {
                category39
            };

            private static List<Guid> categoryGroup8 = new List<Guid>
            {
                category8
            };

            private static List<Guid> categoryGroup9 = new List<Guid>
            {
                category9
            };

            private static List<Guid> categoryGroup10 = new List<Guid>
            {
                category11
            };
            #endregion

            public static List<GroupItem> Execute(WorkflowContext context, DateTime fd, DateTime ld)
            {
                ReportItem reportItem = new ReportItem();
                Dictionary<Guid, Decimal> prices = GetPriceList(context.UserId, fd, ld, context);
                var qb = new QueryBuilder(assignmentDefId, context.UserId);
                qb.Where("PaymentType").Eq(privilege46PaymentId).And("EffectiveDate").Lt(ld).And("ExpiryDate").Gt(fd);
                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var personSrc = query.JoinSource(query.Source, personDefId, SqlSourceJoinType.Inner, "Person");
                query.AddAttribute("&Id", SqlQuerySummaryFunction.Count);
                var catAttr = query.AddAttribute(query.Source, "Category");
                var sexAttr = query.AddAttribute(personSrc, "Sex");
                query.AddGroupAttribute(catAttr);
                query.AddGroupAttribute(sexAttr);

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        var count = reader.IsDbNull(0) ? 0 : reader.GetInt32(0);
                        var categoryId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                        var genderId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                        if (categoryId == Guid.Empty) continue;
                        if (CategoryType.Contains(categoryId))
                        {
                            if (genderId.Equals(new Guid("{C3DCB977-2781-418A-BB96-12FE7F3F041B}")))
                                reportItem = calc(reportItem, categoryId, count, "CountMan");
                            if (genderId.Equals(new Guid("{BC064CB6-0EF7-4535-9208-4288EA6EFD21}")))
                                reportItem = calc(reportItem, categoryId, count, "CountWoman");
                            reportItem = calc(reportItem, categoryId, prices[categoryId], "PrizeSize");
                        }
                    }
                }
                return convertObjectToList(reportItem);
            }

            private static List<GroupItem> convertObjectToList(ReportItem item)
            {
                List<GroupItem> reportItemList = new List<GroupItem>();
                Type type = item.GetType();
                PropertyInfo[] properties = type.GetProperties();
                foreach (PropertyInfo property in properties)
                {
                    reportItemList.Add((GroupItem)property.GetValue(item, null));
                }
                return reportItemList;
            }

            private static ReportItem setValueByProperty(ReportItem item, string reportItemPropertyName, int value, string groupPropertyName)
            {
                if (!string.IsNullOrEmpty(reportItemPropertyName))
                {
                    PropertyInfo propertyInfo = item.GetType().GetProperty(reportItemPropertyName);
                    GroupItem prevValueReportItem = (GroupItem)propertyInfo.GetValue(item, null);
                    PropertyInfo propertyInfoGroup = prevValueReportItem.GetType().GetProperty(groupPropertyName);
                    int prevValueYear = (int)propertyInfoGroup.GetValue(prevValueReportItem, null);
                    propertyInfoGroup.SetValue(prevValueReportItem, prevValueYear + value, null);
                    propertyInfo.SetValue(item, prevValueReportItem, null);
                }
                return item;
            }

            private static ReportItem setValueByProperty(ReportItem item, string reportItemPropertyName, decimal value, string groupPropertyName)
            {
                if (!string.IsNullOrEmpty(reportItemPropertyName))
                {
                    PropertyInfo propertyInfo = item.GetType().GetProperty(reportItemPropertyName);
                    GroupItem prevValueReportItem = (GroupItem)propertyInfo.GetValue(item, null);
                    PropertyInfo propertyInfoGroup = prevValueReportItem.GetType().GetProperty(groupPropertyName);
                    decimal prevValueYear = (decimal)propertyInfoGroup.GetValue(prevValueReportItem, null);
                    propertyInfoGroup.SetValue(prevValueReportItem, prevValueYear + value, null);
                    propertyInfo.SetValue(item, prevValueReportItem, null);
                }
                return item;
            }

            private static ReportItem calc(ReportItem reportItem, Guid categoryId, int count, string PropertyName)
            {
                Type type = typeof(FC_1005);
                foreach (var pi in type.GetFields(System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic))
                {
                    if (isList(pi.FieldType))
                    {
                        var listGuid = (List<Guid>)pi.GetValue(null);
                        if (listGuid.Contains(categoryId))
                            setValueByProperty(reportItem, pi.Name, count, PropertyName);
                    }
                }
                return reportItem;
            }

            private static ReportItem calc(ReportItem reportItem, Guid categoryId, decimal prize, string PropertyName)
            {
                Type type = typeof(FC_1005);
                foreach (var pi in type.GetFields(System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic))
                {
                    if (isList(pi.FieldType))
                    {
                        var listGuid = (List<Guid>)pi.GetValue(null);
                        if (listGuid.Contains(categoryId))
                            setValueByProperty(reportItem, pi.Name, prize, PropertyName);
                    }
                }
                return reportItem;
            }

            private static bool isList(Type type)
            {
                return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>);
            }

            private static Dictionary<Guid, Decimal> GetPriceList(Guid userId, DateTime fd, DateTime ld, WorkflowContext context)
            {
                Dictionary<Guid, Decimal> catList = new Dictionary<Guid, Decimal>();
                QueryBuilder qb = new QueryBuilder(tariffDefId, userId);
                qb.Where("EffectiveDate").Le(fd).And("ExpiryDate").Ge(ld)
                .And("PaymentType").Eq(privilege46PaymentId);
                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("Category");
                query.AddAttribute("Amount");
                using (DataTable table = new DataTable())
                {
                    using (SqlQueryReader reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    foreach (DataRow row in table.Rows)
                    {
                        Guid categoryId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                        if (categoryId != Guid.Empty)
                            if (!catList.ContainsKey(categoryId))
                                catList.Add(categoryId, row[1] is DBNull ? 0m : (decimal)row[1]);
                            else
                                throw new ApplicationException("Ошибка при чтении тарифов ДК. Сообщите администрации об этой ошибке.");
                    }
                }
                return catList;
            }

            [DataContract]
            public class GroupItem
            {
                [DataMember]
                public string CategoryName { get; set; }
                [DataMember]
                public int CountMan { get; set; }
                [DataMember]
                public int CountWoman { get; set; }
                [DataMember]
                public decimal PrizeSize { get; set; }

                public GroupItem(string categoryName)
                {
                    CategoryName = categoryName;
                }
            }

            public class ReportItem
            {
                public ReportItem()
                {
                    categoryGroup1 = new GroupItem("Инвалид Советской армии при исполнении служеб. Обязанности");
                    categoryGroup2 = new GroupItem("Инв.-участник ликвидации последствий аварии на ЧАЭC");
                    categoryGroup3 = new GroupItem("ЛОВЗ по слуху");
                    categoryGroup4 = new GroupItem("ЛОВЗ по зрению");
                    categoryGroup5 = new GroupItem("ЛОВЗ по слуху и зрению до 18 лет");
                    categoryGroup6 = new GroupItem("Почетный донор");
                    categoryGroup7 = new GroupItem("Дети участника ЧАЭС в возрасте до 18 лет");
                    categoryGroup8 = new GroupItem("Семьям погибших военнослужащих при исполнении обязанностей военной службы");
                    categoryGroup9 = new GroupItem("Семья, потерявшая кормильца - участника ЧАЭС");
                    categoryGroup10 = new GroupItem("Семьи сотрудников МВД, погибших при исполнении сл. обяз. или сл.долга");
                }
                public GroupItem categoryGroup1 { get; set; }
                public GroupItem categoryGroup2 { get; set; }
                public GroupItem categoryGroup3 { get; set; }
                public GroupItem categoryGroup4 { get; set; }
                public GroupItem categoryGroup5 { get; set; }
                public GroupItem categoryGroup6 { get; set; }
                public GroupItem categoryGroup7 { get; set; }
                public GroupItem categoryGroup8 { get; set; }
                public GroupItem categoryGroup9 { get; set; }
                public GroupItem categoryGroup10 { get; set; }
            }
        }

        public static class MonthlyGrant_1017
        {
            private static readonly Guid uyBulogoKomokPaymentId = new Guid("{3193A90D-380B-4428-B7B1-04C548AA902E}");   // * Уй-булого комок
            // Document Defs Id 1e750c67-2ddf-488e-a4c4-d94547433067
            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");// "{04D25808-6DE9-42F5-8855-6F68A94A224C}"
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{AD72B200-60BA-412B-ADBF-F32D3BE2D40C}");
            private static readonly Guid reportItemDefId = new Guid("{F3FCDA28-FEBB-43A0-AD73-848BB175089E}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");
            // States
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid canceledStateId = new Guid("{C65E63CC-54E9-424E-AFDC-5A8DB1CB04A3}"); // Аннулирован
            private static readonly Guid onApprovingStateId = new Guid("{5CD9E88D-671E-4A44-AD92-9F74DA3B47F7}"); //На утверждении
            private static readonly Guid onRegisteringStateId = new Guid("{A9CD37C4-A718-4DE1-9E95-EC8EC280C8D4}"); // На регистрации
            private static readonly Guid refusedStateId = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}"); // Отказан
            private static readonly Guid personDefId = new Guid("{6F5B8A06-361E-4559-8A53-9CB480A9B16C}");
            private static readonly Guid privilege46PaymentId = new Guid("{7BEFD6DA-042C-4A77-90F3-A4424033E4DD}");

            private static readonly Guid twinsId = new Guid("{45D55628-5E72-42B8-8B8D-667346E79046}");
            private static readonly Guid tripleId = new Guid("{8CCAE21E-128A-4728-9479-9C094271C614}");

            private static readonly Guid childUpTo3YearsId = new Guid("{D8FF3DAF-A701-414A-B965-4BF93BB658B9}");
            private static readonly Guid childFrom3To16YearId = new Guid("{D8FF3DAF-A701-414A-B965-4BF93BB658B9}");
            private static readonly Guid studentUpto23YearId = new Guid("{AF79BAD1-83D8-4EEA-8FD4-189342735075}");
            private static readonly Guid studentOlder23YearId = new Guid("{46AEAEA2-D42F-47C9-A571-9F3789413CC3}");
            private static readonly Guid studentOfCommercialUnivercityId = new Guid("{AF79BAD1-83D8-4EEA-8FD4-189342735075}");

            private static readonly List<Guid> studentList = new List<Guid>
            {
                studentUpto23YearId,studentOlder23YearId,studentOfCommercialUnivercityId
            };

            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {
                var items = new List<ReportItem>();
                var fd = new DateTime(year, month, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var qb = new QueryBuilder(assignmentDefId, context.UserId);
                qb.Where("PaymentType").Eq(uyBulogoKomokPaymentId).And("EffectiveDate").Lt(ld).And("ExpiryDate").Gt(fd);

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);

                query.AddAttribute("&OrgId");
                query.AddAttribute("MembershipType");
                query.AddAttribute("EmploymentStatus");
                query.AddAttribute("Amount");

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        var orgId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                        var membershipTypeId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                        var employmentStatusId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                        var amount = reader.IsDbNull(3) ? 0m : reader.GetDecimal(3);

                        if (orgId == Guid.Empty) continue;
                        var orgName = context.Orgs.GetOrgName(orgId);
                        var item = GetReportItem(items, orgName);
                        if (membershipTypeId.Equals(twinsId) || membershipTypeId.Equals(tripleId))
                        {
                            if (membershipTypeId.Equals(twinsId) && employmentStatusId.Equals(childUpTo3YearsId))
                                item.twinsUpTo3Years += 1;

                            if (membershipTypeId.Equals(tripleId) && employmentStatusId.Equals(childUpTo3YearsId))
                                item.tripletUpTo3Year += 1;

                            if (membershipTypeId.Equals(tripleId) && employmentStatusId.Equals(childFrom3To16YearId))
                                item.tripletFrom3To16Year += 1;

                            item.grantSizeForMonthFirstGroup += amount;
                        }
                        else
                        {
                            if (employmentStatusId.Equals(childUpTo3YearsId))
                            {
                                item.childUpto3Years += 1;
                                item.grantSizeForMonthSecondGroup += amount;
                            }

                            if (employmentStatusId.Equals(childFrom3To16YearId))
                            {
                                item.childFrom3To16Year += 1;
                                item.grantSizeForMonthSecondGroup += amount;
                            }

                            if (studentList.Contains(employmentStatusId))
                            {
                                item.students += 1;
                                item.grantSizeForMonthSecondGroup += amount;
                            }
                        }

                    }

                }
                foreach (var subItem in items)
                {
                    subItem.countGrantReceipientFirstGroup = subItem.twinsUpTo3Years + subItem.tripletUpTo3Year + subItem.tripletFrom3To16Year;
                    subItem.countGrantReceipientSecondGroup = subItem.childUpto3Years + subItem.childFrom3To16Year + subItem.students;
                    if (subItem.countGrantReceipientFirstGroup == 0)
                        subItem.averageGrantFirstGroup = 0;
                    else subItem.averageGrantFirstGroup = subItem.grantSizeForMonthFirstGroup / subItem.countGrantReceipientFirstGroup;
                    if (subItem.countGrantReceipientSecondGroup == 0)
                        subItem.averageGrantSecondGroup = 0;
                    else subItem.averageGrantSecondGroup = subItem.grantSizeForMonthSecondGroup / subItem.countGrantReceipientSecondGroup;

                    subItem.totalCountGrantRecipient = subItem.countGrantReceipientFirstGroup + subItem.countGrantReceipientSecondGroup;
                    if (subItem.totalCountGrantRecipient == 0)
                        subItem.generalAverageGrantToPerMan = 0;
                    else subItem.generalAverageGrantToPerMan = (subItem.grantSizeForMonthFirstGroup + subItem.grantSizeForMonthSecondGroup) / subItem.totalCountGrantRecipient;
                }
                var executeApplicant = ExecuteApplicant(context, year, month);
                foreach (var subItem in executeApplicant)
                {
                    var item = items.Where(x => x.orgName.Equals(subItem.orgName)).FirstOrDefault();
                    if (item != null)
                    {
                        item.applicantCount = subItem.applicantCount;
                    }
                }
                var executeGetSinceBeginOfYear = ExecuteGetSinceBeginOfYear(context, year, month);
                foreach (var subItem in executeGetSinceBeginOfYear)
                {
                    var item = items.Where(x => x.orgName.Equals(subItem.orgName)).FirstOrDefault();
                    if (item != null)
                    {
                        item.grantTotalSinceBeginOfYearFirstGroup += subItem.grantTotalSinceBeginOfYearFirstGroup;
                        item.grantTotalSinceBeginOfYearSecondGroup += subItem.grantTotalSinceBeginOfYearSecondGroup;
                    }
                }

                return items;
            }

            public static List<ReportItem> ExecuteGetSinceBeginOfYear(WorkflowContext context, int year, int month)
            {
                var items = new List<ReportItem>();
                var fd = new DateTime(year, 1, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var qb = new QueryBuilder(assignmentDefId, context.UserId);
                qb.Where("PaymentType").Eq(uyBulogoKomokPaymentId).And("EffectiveDate").Lt(ld).And("ExpiryDate").Gt(fd);

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);

                query.AddAttribute("&OrgId");
                query.AddAttribute("MembershipType");
                query.AddAttribute("EmploymentStatus");
                query.AddAttribute("Amount");

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        var orgId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                        var membershipTypeId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                        var employmentStatusId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                        var amount = reader.IsDbNull(3) ? 0m : reader.GetDecimal(3);

                        if (orgId == Guid.Empty) continue;
                        var orgName = context.Orgs.GetOrgName(orgId);
                        var item = GetReportItem(items, orgName);
                        if (membershipTypeId.Equals(twinsId) || membershipTypeId.Equals(tripleId))
                        {
                            item.grantTotalSinceBeginOfYearFirstGroup += amount;
                        }
                        else
                        {
                            if (employmentStatusId.Equals(childUpTo3YearsId))
                            {
                                item.grantTotalSinceBeginOfYearSecondGroup += amount;
                            }

                            if (employmentStatusId.Equals(childFrom3To16YearId))
                            {
                                item.grantTotalSinceBeginOfYearSecondGroup += amount;
                            }

                            if (studentList.Contains(employmentStatusId))
                            {
                                item.grantTotalSinceBeginOfYearSecondGroup += amount;
                            }

                        }

                    }

                }

                return items;
            }


            public static List<ReportItem> ExecuteApplicant(WorkflowContext context, int year, int month)
            {
                var items = new List<ReportItem>();
                var fd = new DateTime(year, month, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var qb = new QueryBuilder(appDefId, context.UserId);
                qb.Where("PaymentType").Eq(uyBulogoKomokPaymentId).And("AssignFrom").Lt(ld).And("AssignToMax").Gt(fd);

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("&Id", SqlQuerySummaryFunction.Count);
                query.AddAttribute("&OrgId");
                query.AddAttribute("Applicant");
                query.AddGroupAttribute("&OrgId");
                query.AddGroupAttribute("Applicant");

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        var count = reader.IsDbNull(0) ? 0 : reader.GetInt32(0);
                        var orgId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                        var applicantId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                        if (orgId == Guid.Empty) continue;
                        var orgName = context.Orgs.GetOrgName(orgId);
                        var item = GetReportItem(items, orgName);
                        item.applicantCount += count;
                    }
                }
                return items;
            }
            static ReportItem GetReportItem(List<ReportItem> items, string orgName)
            {
                var item = items.FirstOrDefault(x => x.orgName == orgName);
                if (item != null) return item;
                item = new ReportItem
                {
                    orgName = orgName,
                    rowNo = items.Count > 0 ? (items.Max(x => x.rowNo) + 1) : 1
                };
                items.Add(item);
                return item;
            }
            public class ReportItem
            {
                public int rowNo { get; set; }
                public string orgName { get; set; }
                public int applicantCount { get; set; }
                public int twinsUpTo3Years { get; set; }
                public int tripletUpTo3Year { get; set; }
                public int tripletFrom3To16Year { get; set; }
                public int countGrantReceipientFirstGroup { get; set; }
                public decimal averageGrantFirstGroup { get; set; }
                public decimal grantSizeForMonthFirstGroup { get; set; }
                public decimal grantTotalSinceBeginOfYearFirstGroup { get; set; }
                public int childUpto3Years { get; set; }
                public int childFrom3To16Year { get; set; }
                public int students { get; set; }
                public int countGrantReceipientSecondGroup { get; set; }
                public decimal averageGrantSecondGroup { get; set; }
                public decimal grantSizeForMonthSecondGroup { get; set; }
                public decimal grantTotalSinceBeginOfYearSecondGroup { get; set; }
                public int totalCountGrantRecipient { get; set; }
                public decimal generalAverageGrantToPerMan { get; set; }
            }

        }

        public static class Event2010_1018
        {
            private static readonly Guid despPaymentTypeId = new Guid("{272F8D91-5B4B-42AC-9D79-A17E77F5496E}");
            private static readonly Guid uyBulogoKomokPaymentId = new Guid("{3193A90D-380B-4428-B7B1-04C548AA902E}");   // * Уй-булого комок
            // Document Defs Id 1e750c67-2ddf-488e-a4c4-d94547433067
            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");// "{04D25808-6DE9-42F5-8855-6F68A94A224C}"
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{AD72B200-60BA-412B-ADBF-F32D3BE2D40C}");
            private static readonly Guid reportItemDefId = new Guid("{F3FCDA28-FEBB-43A0-AD73-848BB175089E}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");
            // States
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid canceledStateId = new Guid("{C65E63CC-54E9-424E-AFDC-5A8DB1CB04A3}"); // Аннулирован
            private static readonly Guid onApprovingStateId = new Guid("{5CD9E88D-671E-4A44-AD92-9F74DA3B47F7}"); //На утверждении
            private static readonly Guid onRegisteringStateId = new Guid("{A9CD37C4-A718-4DE1-9E95-EC8EC280C8D4}"); // На регистрации
            private static readonly Guid refusedStateId = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}"); // Отказан

            private static readonly Guid childUpto18YearsId = new Guid("{608F740E-C44B-4305-88E8-F53D497214C0}");
            private static readonly Guid disabledId = new Guid("{7D08F570-239D-46BF-BF4F-6E3A510ACCD2}");
            private static readonly Guid parentId = new Guid("{238BD896-A3B7-494D-B549-4AAC292EE8F5}");
            private static readonly Guid heavyWoundAprilId = new Guid("{AA83612B-0538-49A3-8841-0F42E7DFCDA6}");
            private static readonly Guid fleshWoundAprilId = new Guid("{2F488FCE-E02E-4875-870C-ABDBF320B726}");
            private static readonly Guid heavyWoundMayId = new Guid("{CEF4185C-77AE-4B7D-BFB7-74598F9F0462}");
            private static readonly Guid fleshWoundMayId = new Guid("{D910F004-ED3B-4A6D-BDC4-7C51854954F6}");
            private static readonly Guid heavyWoundJuneId = new Guid("{6E50CFEE-7F02-4253-9C33-82902EF9192D}");
            private static readonly Guid fleshWoundJuneId = new Guid("{2281A6BD-50DF-49FA-9B8B-11F1F4532B0F}");
            private static readonly Guid familiesOfDeadInAprilEventId = new Guid("{1C4924D0-CAEE-4D65-9B42-1AC992D61326}");
            private static readonly Guid familiesOfDeadInMayEventId = new Guid("{687DAC74-5AF9-4C32-86A8-5ED60873125C}");
            private static readonly Guid familiesOfDeadInJuneEventId = new Guid("{E415799C-2C5B-4FBF-B367-16CC246BEEB3}");
            private static readonly Guid familiesOfMissingInJuneEventId = new Guid("{1FFEA278-CD71-4E9C-81A6-33C2E1581A17}");

            private static readonly List<Guid> categoryList = new List<Guid>
            {
                childUpto18YearsId,disabledId,parentId,heavyWoundAprilId,fleshWoundAprilId,heavyWoundMayId,fleshWoundJuneId,
                familiesOfDeadInAprilEventId,familiesOfDeadInMayEventId,familiesOfDeadInJuneEventId,familiesOfMissingInJuneEventId
            };


            private static readonly List<Guid> Event2010List = new List<Guid>
            {
                childUpto18YearsId,disabledId,parentId
            };
            private static readonly List<Guid> juneEventList = new List<Guid>
            {
                heavyWoundJuneId,fleshWoundJuneId,familiesOfDeadInJuneEventId,familiesOfMissingInJuneEventId
            };

            private static readonly List<Guid> aprilEventList = new List<Guid>
            {
                heavyWoundAprilId,fleshWoundAprilId,familiesOfDeadInAprilEventId
            };
            private static readonly List<Guid> mayEventList = new List<Guid>
            {
                heavyWoundMayId,fleshWoundMayId,familiesOfDeadInMayEventId
            };
            public static List<GroupReportItem> Execute(WorkflowContext context, int year, int month)
            {
                var items = new List<GroupReportItem>();
                var fd = new DateTime(year, month, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var qb = new QueryBuilder(assignmentDefId, context.UserId);
                qb.Where("PaymentType").Eq(despPaymentTypeId).And("EffectiveDate").Lt(ld).And("ExpiryDate").Gt(fd);
                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("&Id", SqlQuerySummaryFunction.Count);
                query.AddAttribute("Amount", SqlQuerySummaryFunction.Sum);
                query.AddAttribute("&OrgId");
                query.AddAttribute("Category");
                query.AddGroupAttribute("&OrgId");
                query.AddGroupAttribute("Category");

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        var count = reader.IsDbNull(0) ? 0 : reader.GetInt32(0);
                        var sum = reader.IsDbNull(1) ? 0m : reader.GetDecimal(1);
                        var orgId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                        var categoryId = reader.IsDbNull(3) ? Guid.Empty : reader.GetGuid(3);

                        if (orgId == Guid.Empty) continue;
                        var orgName = context.Orgs.GetOrgName(orgId);
                        var item = GetGroupReportItem(items, orgName);

                        if (categoryList.Contains(categoryId))
                        {
                            if (childUpto18YearsId.Equals(categoryId))
                            {
                                item.currentItem.childUpto18Years += count;
                                item.totalSum += sum;
                                item.totalCount += count;
                            }
                            if (disabledId.Equals(categoryId))
                            {
                                item.currentItem.disabled += count;
                                item.totalSum += sum;
                                item.totalCount += count;
                            }
                            if (parentId.Equals(categoryId))
                            {
                                item.currentItem.parent += count;
                                item.totalSum += sum;
                                item.totalCount += count;
                            }
                            if (aprilEventList.Contains(categoryId))
                            {
                                item.firstItem += count;
                                item.totalSum += sum;
                                item.totalCount += count;
                            }
                            if (mayEventList.Contains(categoryId))
                            {
                                item.secondItem += count;
                                item.totalSum += sum;
                                item.totalCount += count;
                            }
                            if (juneEventList.Contains(categoryId))
                            {
                                item.thirdItem += count;
                                item.totalSum += sum;
                                item.totalCount += count;
                            }
                        }
                    }

                }

                return items;
            }


            static GroupReportItem GetGroupReportItem(List<GroupReportItem> items, string orgName)
            {
                var item = items.FirstOrDefault(x => x.orgName == orgName);
                if (item != null) return item;
                item = new GroupReportItem
                {
                    orgName = orgName,
                    rowNo = items.Count > 0 ? (items.Max(x => x.rowNo) + 1) : 1
                };
                items.Add(item);
                return item;
            }

            public class ReportItem
            {
                public int childUpto18Years { get; set; }
                public int disabled { get; set; }
                public int parent { get; set; }
            }

            public class GroupReportItem
            {
                public GroupReportItem()
                {
                    currentItem = new ReportItem();
                }
                public int rowNo { get; set; }
                public string orgName { get; set; }
                public int totalCount { get; set; }
                public decimal totalSum { get; set; }
                public ReportItem currentItem { get; set; }
                public int firstItem { get; set; }
                public int secondItem { get; set; }
                public int thirdItem { get; set; }
            }
        }

        public static class Report_1023
        {
            private static readonly Guid uyBulogoKomokPaymentId = new Guid("{3193A90D-380B-4428-B7B1-04C548AA902E}");

            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{1A09ECD6-55E1-4307-862E-6F98F47E252C}");
            private static readonly Guid reportItemDefId = new Guid("{2A8709AB-3522-4019-A29F-5C333893645B}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");

            // States                               
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid canceledStateId = new Guid("{C65E63CC-54E9-424E-AFDC-5A8DB1CB04A3}"); // Аннулирован
            private static readonly Guid onApprovingStateId = new Guid("{5CD9E88D-671E-4A44-AD92-9F74DA3B47F7}"); //На утверждении
            private static readonly Guid onRegisteringStateId = new Guid("{A9CD37C4-A718-4DE1-9E95-EC8EC280C8D4}"); // На регистрации
            private static readonly Guid refusedStateId = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}"); // Отказан
            private static readonly Guid onPaymentStateTypeId = new Guid("{3DB4DB00-3A7F-4228-A9A3-A413B85C18B4}"); //Оформлен

            //RefuseReasonType
            private static readonly Guid deniedYesDurableGoodsId = new Guid("{EEF4A0D7-9763-4E39-907E-DCC46C929DF0}");
            private static readonly Guid deniedYesFarmAnimalsId = new Guid("{690F141C-2FCD-4014-B309-C7106C0FE1FF}");
            private static readonly Guid deniedFAPCIHigherThanGMI = new Guid("{A71ABA04-F07B-4AFB-B372-24EF87EEAC62}");
            private static readonly Guid deniedNoKidsUpTo18Year = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}");
            private static readonly Guid deniedOtherReason = new Guid("{E7D459E6-C63F-4EEB-A7E8-B4B9A82FB0D0}");
            private static readonly Guid confirmedFamilyWithRetiree = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}");

            static Guid familyMemberDefId = new Guid("{85B03F9E-47D7-4829-8041-0CDCB8486572}");
            static Guid pensionerEnumId = new Guid("{CC14F361-689D-49B7-AC9B-259B0E84C399}");
            static Guid emplPensionerId = new Guid("{8617FB6E-6C54-4C6E-97C3-1A7D2887AA30}");

            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {
                var items = new List<ReportItem>();
                var fd = new DateTime(year, month, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var qb = new QueryBuilder(appDefId, context.UserId);
                qb.Where("PaymentType").Eq(uyBulogoKomokPaymentId).And("&State").Eq(refusedStateId).And("RegDate").Ge(fd).And("RegDate").Le(ld);
                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("&Id", SqlQuerySummaryFunction.Count);
                query.AddAttribute("&OrgId");
                query.AddAttribute("RefuseReason");
                query.AddGroupAttribute("&OrgId");
                query.AddGroupAttribute("RefuseReason");

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        var count = reader.IsDbNull(0) ? 0 : reader.GetInt32(0);
                        var orgId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                        var refuseReasonId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);

                        if (orgId == Guid.Empty) continue;
                        var orgName = context.Orgs.GetOrgName(orgId);
                        var item = GetReportItem(items, orgName);
                        if (refuseReasonId.Equals(deniedYesDurableGoodsId))
                        {
                            item.deniedYesDurableGoods += count;
                        }
                        if (refuseReasonId.Equals(deniedYesFarmAnimalsId))
                        {
                            item.deniedYesFarmAnimals += count;
                        }
                        if (refuseReasonId.Equals(deniedFAPCIHigherThanGMI))
                        {
                            item.deniedFAPCIHigherThanGMI += count;
                        }
                        if (refuseReasonId.Equals(deniedNoKidsUpTo18Year))
                        {
                            item.deniedNoKidsUpTo18Year += count;
                        }
                        if (refuseReasonId.Equals(deniedOtherReason))
                        {
                            item.deniedOtherReason += count;
                        }
                        item.countAllDenied += count;
                    }
                }
                GetConfirmedAppBaseData(context, year, month, items);

                return items;
            }


            public static void GetConfirmedAppBaseData(WorkflowContext context, int year, int month, List<ReportItem> items)
            {
                var fd = new DateTime(year, month, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var qb = new QueryBuilder(appDefId, context.UserId);
                qb.Where("PaymentType").Eq(uyBulogoKomokPaymentId).And("&State")
                    .In(new object[]
                    {
                        approvedStateId,
                        onApprovingStateId,
                        //onRegisteringStateId,
                        onPaymentStateTypeId
                    })
                    .And("RegDate").Ge(fd).And("RegDate").Le(ld);
                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var fMemSrc = query.JoinSource(query.Source, familyMemberDefId, SqlSourceJoinType.Inner, "FamilyMembers");
                query.AddAttribute(query.Source, "&Id");
                query.AddAttribute(query.Source, "&OrgId");
                query.AddAttribute(query.Source, "EmploymentStatus");
                query.AddAttribute(fMemSrc, "EmploymentStatus");

                var table = new DataTable();

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }
                var tempList = new HashSet<Guid>();
                foreach (DataRow row in table.Rows)
                {
                    var appID = (Guid)row[0];
                    var orgId = (Guid)row[1];
                    var appStatusId = row[2] is DBNull ? Guid.Empty : (Guid)row[2];
                    var fMemStatusId = row[3] is DBNull ? Guid.Empty : (Guid)row[3];

                    if (orgId == Guid.Empty) continue;

                    var orgName = context.Orgs.GetOrgName(orgId);
                    var item = GetReportItem(items, orgName);
                    bool isInList = tempList.Any(x => x == appID);
                    if ((appStatusId == pensionerEnumId ||
                        appStatusId == emplPensionerId ||
                        fMemStatusId == pensionerEnumId ||
                        fMemStatusId == emplPensionerId) && !isInList)
                    {
                        item.confirmedFamilyWithRetiree++;
                    }
                    if (!isInList)
                    {
                        tempList.Add(appID);
                        item.countAllConfirmed++;
                    }
                }
            }

            static ReportItem GetReportItem(List<ReportItem> items, string orgName)
            {
                var item = items.FirstOrDefault(x => x.orgName == orgName);
                if (item != null) return item;
                item = new ReportItem
                {
                    orgName = orgName,
                    rowNo = items.Count > 0 ? (items.Max(x => x.rowNo) + 1) : 1
                };
                items.Add(item);
                return item;
            }

            public class ReportItem
            {
                public int rowNo { get; set; }
                public string orgName { get; set; }
                public int count { get; set; }
                public int deniedYesDurableGoods { get; set; }
                public int deniedYesFarmAnimals { get; set; }
                public int deniedFAPCIHigherThanGMI { get; set; }// family's average per capita income higher than  guaranteed minimum income
                public int deniedNoKidsUpTo18Year { get; set; }
                public int deniedOtherReason { get; set; }
                public int countAllDenied { get; set; }
                public int confirmedFamilyWithRetiree { get; set; }
                public int countAllConfirmed { get; set; }
            }

        }

        public static class Report_1020_2
        {
            private static readonly Guid uyBulogoKomokPaymentId = new Guid("{3193A90D-380B-4428-B7B1-04C548AA902E}");

            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{1A09ECD6-55E1-4307-862E-6F98F47E252C}");
            private static readonly Guid reportItemDefId = new Guid("{2A8709AB-3522-4019-A29F-5C333893645B}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");

            private static readonly Guid retirementBenefitPaymentId = new Guid("{AFAA7F86-74AE-4260-9933-A56F7845E55A}");  // ЕСП престарелым гражданам
            private static readonly Guid childBenefitPaymentId = new Guid("{AB3F8C41-897A-4574-BAA0-B7CD4AAA1C80}");  // ЕСП на инвалида - члена семьи
            private static readonly Guid invalidBenefitPaymentId = new Guid("{70C28E62-2387-4A59-917D-A366ADE119A8}");  // ЕСП по инвалидности
            private static readonly Guid survivorBenefitPaymentId = new Guid("{839D5712-E75B-4E71-83F7-168CE4F089C0}");  // ЕСП детям при утере кормильца 
            private static readonly Guid aidsFromBenefitPaymentId = new Guid("{3BEBE4F9-0B15-41CB-9B96-54E83819AB0F}");  // ЕСП детям, инфецированным ВИЧ/СПИД
            private static readonly Guid aidsBenefitPaymentId = new Guid("{47EEBFBC-A4E9-495D-A6A1-F87B5C3057C9}");  // ЕСП детям от матерей с ВИЧ/СПИД до 18 месяцев
            private static readonly Guid orphanBenefitPaymentId = new Guid("{4F12C366-7E2F-4208-9CB8-4EAB6E6C0EF1}");   // ЕСП круглым сиротам 

            // States                               
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid canceledStateId = new Guid("{C65E63CC-54E9-424E-AFDC-5A8DB1CB04A3}"); // Аннулирован
            private static readonly Guid onApprovingStateId = new Guid("{5CD9E88D-671E-4A44-AD92-9F74DA3B47F7}"); //На утверждении
            private static readonly Guid onRegisteringStateId = new Guid("{A9CD37C4-A718-4DE1-9E95-EC8EC280C8D4}"); // На регистрации
            private static readonly Guid refusedStateId = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}"); // Отказан
            private static readonly Guid onPaymentStateTypeId = new Guid("{3DB4DB00-3A7F-4228-A9A3-A413B85C18B4}"); //Оформлен


            private static readonly Guid bankId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid applicantId = new Guid("{6F5B8A06-361E-4559-8A53-9CB480A9B16C}");
            private static readonly decimal postStaticPercent = 1.3m;

            private static List<Guid> benefitPaymentList = new List<Guid>
            {
                retirementBenefitPaymentId,childBenefitPaymentId,invalidBenefitPaymentId,survivorBenefitPaymentId,
                aidsFromBenefitPaymentId,aidsBenefitPaymentId,orphanBenefitPaymentId
            };
            public static List<ReportItem> Execute(WorkflowContext context, DateTime fd, DateTime ld)
            {
                var items = new List<ReportItem>();
                //var fd = new DateTime(year, month, 1);
                //var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var qb = new QueryBuilder(appDefId, context.UserId);
                qb.Where("PaymentType")
                    .In(new object[]
                    {
                    uyBulogoKomokPaymentId,
                    retirementBenefitPaymentId,
                    childBenefitPaymentId,
                    survivorBenefitPaymentId,
                    aidsFromBenefitPaymentId,
                    aidsBenefitPaymentId,
                    orphanBenefitPaymentId
                    })
                    .And("&State")
                    .In(new object[]
                    {
                        approvedStateId,
                        onApprovingStateId,
                        onRegisteringStateId,
                        onPaymentStateTypeId
                    })
                    .And("RegDate").Ge(fd).And("RegDate").Le(ld);
                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var bankSrc = query.JoinSource(query.Source, bankId, SqlSourceJoinType.LeftOuter, "Bank");
                var applicantSrc = query.JoinSource(query.Source, applicantId, SqlSourceJoinType.Inner, "Applicant");
                query.AddAttribute("&Id");
                query.AddAttribute("&OrgId");
                query.AddAttribute("PaymentType");
                query.AddAttribute(bankSrc, "Percent");
                query.AddAttribute("PaymentSum");
                query.AddAttribute("DependentCount");
                query.AddAttribute(applicantSrc, "Citizenship");

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        //if (bankSrc == Guid.Empty) continue;
                        var appId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                        var orgId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                        var paymentId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                        var percentOfBank = reader.IsDbNull(3) ? 0f : reader.GetDouble(3);

                        var amount = reader.IsDbNull(4) ? 0m : reader.GetDecimal(4);
                        var countUBKPerson = reader.IsDbNull(5) ? 0 : reader.GetInt32(5);
                        var citizenshipId = reader.IsDbNull(6) ? Guid.Empty : reader.GetGuid(6);
                        if (orgId == Guid.Empty) continue;
                        var orgName = context.Orgs.GetOrgName(orgId);
                        var item = GetReportItem(items, orgName);
                        if (paymentId.Equals(uyBulogoKomokPaymentId))
                        {
                            item.countUBKPerson += countUBKPerson;
                            item.countUBKFamily += 1;
                            item.averageAmountUBK += amount;
                            if (percentOfBank > 0)
                            {
                                item.averageSumUBKTIAS += (amount + Math.Round((Convert.ToDecimal(percentOfBank) * Convert.ToDecimal(amount)) / 100, 2));
                            }
                            else
                            {
                                item.averageSumUBKTIAS += (amount + Math.Round((postStaticPercent * amount) / 100, 2));
                            }
                        }
                        if (benefitPaymentList.Contains(paymentId))
                        {
                            item.countMSBFamily += 1;
                            item.averageAmountMSB += amount;
                            if (percentOfBank > 0)
                            {
                                item.averageSumMSBTIAS += (amount + Math.Round((Convert.ToDecimal(percentOfBank) * Convert.ToDecimal(amount)) / 100, 2));
                            }
                            else
                            {
                                item.averageSumMSBTIAS += (amount + Math.Round((postStaticPercent * amount) / 100, 2));
                            }
                        }
                        if (citizenshipId.Equals(new Guid("{70A7AFF3-D6AE-4A9E-92B7-C850487DBFB2}")))
                            item.Uzbekistan += 1;
                        if (citizenshipId.Equals(new Guid("{DDD66E3B-4731-41D0-99CD-5C13C4B1DFAB}")))
                            item.Tajikistan += 1;
                        if (citizenshipId.Equals(new Guid("{0DDD3EFE-67B3-4D58-9A85-26EF0F68C9B8}")))
                            item.Belarus += 1;
                        if (citizenshipId.Equals(new Guid("{4A4FF97B-EFE0-4CAB-97B6-BAD9F7E8E6F6}")))
                            item.Armenia += 1;
                        if (citizenshipId.Equals(new Guid("{89294ABB-88D9-432A-AFBE-CC7FAF63A77A}")))
                            item.Moldova += 1;
                        if (citizenshipId.Equals(new Guid("{6D47C8D9-BD38-45BC-83C2-789D2D20FB31}")))
                            item.Kazakhstan += 1;
                        if (citizenshipId.Equals(new Guid("{70F5BD8E-9CF8-412D-BE52-7284EB2888E2}")))
                            item.Russia += 1;
                        if (citizenshipId.Equals(new Guid("{D9924C3B-6081-4487-9857-0AD2C0A4F9EE}")))
                            item.Ukraine += 1;
                        if (citizenshipId.Equals(new Guid("{EA91668C-9C5B-49BB-BCAD-3FB329766C8A}")))
                            item.Others += 1;
                    }
                }
                foreach (var item in items)
                {
                    item.countAllForeignCitizen = item.Uzbekistan + item.Tajikistan + item.Belarus + item.Armenia + item.Moldova + item.Kazakhstan + item.Russia + item.Ukraine + item.Others;
                    var countMSBFamily = 0;
                    var countUBKFamily = 0;
                    if (item.countMSBFamily == 0) countMSBFamily = 1;
                    else countMSBFamily = item.countMSBFamily;
                    if (item.countUBKFamily == 0) countUBKFamily = 1;
                    else countUBKFamily = item.countUBKFamily;
                    item.averageAmountMSB = Math.Round(item.averageAmountMSB / countMSBFamily, 2);
                    item.averageAmountUBK = Math.Round(item.averageAmountUBK / countUBKFamily, 2);
                }
                return items;
            }

            static ReportItem GetReportItem(List<ReportItem> items, string orgName)
            {
                var item = items.FirstOrDefault(x => x.orgName == orgName);
                if (item != null) return item;
                item = new ReportItem
                {
                    orgName = orgName,
                    rowNo = items.Count > 0 ? (items.Max(x => x.rowNo) + 1) : 1
                };
                items.Add(item);
                return item;
            }
            public class ReportItem
            {
                public int rowNo { get; set; }
                public string orgName { get; set; }
                public int countAllForeignCitizen { get; set; }
                public int countUBKFamily { get; set; }
                public int countUBKPerson { get; set; }
                public decimal averageAmountUBK { get; set; } //UBK-Uy bulogo komok
                public decimal averageSumUBKTIAS { get; set; } // TIAS-taking into account services
                public int countMSBFamily { get; set; } //MSB-monthly social benefit
                public decimal averageAmountMSB { get; set; }
                public decimal averageSumMSBTIAS { get; set; }
                public int Uzbekistan { get; set; }
                public int Tajikistan { get; set; }
                public int Belarus { get; set; }
                public int Armenia { get; set; }
                public int Moldova { get; set; }
                public int Kazakhstan { get; set; }
                public int Russia { get; set; }
                public int Ukraine { get; set; }
                public int Others { get; set; }
            }
        }

        public static class Report_1020
        {
            private static readonly Guid uyBulogoKomokPaymentId = new Guid("{3193A90D-380B-4428-B7B1-04C548AA902E}");

            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{1A09ECD6-55E1-4307-862E-6F98F47E252C}");
            private static readonly Guid reportItemDefId = new Guid("{2A8709AB-3522-4019-A29F-5C333893645B}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");

            private static readonly Guid retirementBenefitPaymentId = new Guid("{AFAA7F86-74AE-4260-9933-A56F7845E55A}");  // ЕСП престарелым гражданам
            private static readonly Guid childBenefitPaymentId = new Guid("{AB3F8C41-897A-4574-BAA0-B7CD4AAA1C80}");  // ЕСП на инвалида - члена семьи
            private static readonly Guid invalidBenefitPaymentId = new Guid("{70C28E62-2387-4A59-917D-A366ADE119A8}");  // ЕСП по инвалидности
            private static readonly Guid survivorBenefitPaymentId = new Guid("{839D5712-E75B-4E71-83F7-168CE4F089C0}");  // ЕСП детям при утере кормильца 
            private static readonly Guid aidsFromBenefitPaymentId = new Guid("{3BEBE4F9-0B15-41CB-9B96-54E83819AB0F}");  // ЕСП детям, инфецированным ВИЧ/СПИД
            private static readonly Guid aidsBenefitPaymentId = new Guid("{47EEBFBC-A4E9-495D-A6A1-F87B5C3057C9}");  // ЕСП детям от матерей с ВИЧ/СПИД до 18 месяцев
            private static readonly Guid orphanBenefitPaymentId = new Guid("{4F12C366-7E2F-4208-9CB8-4EAB6E6C0EF1}");   // ЕСП круглым сиротам 

            // States                               
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid canceledStateId = new Guid("{C65E63CC-54E9-424E-AFDC-5A8DB1CB04A3}"); // Аннулирован
            private static readonly Guid onApprovingStateId = new Guid("{5CD9E88D-671E-4A44-AD92-9F74DA3B47F7}"); //На утверждении
            private static readonly Guid onRegisteringStateId = new Guid("{A9CD37C4-A718-4DE1-9E95-EC8EC280C8D4}"); // На регистрации
            private static readonly Guid refusedStateId = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}"); // Отказан
            private static readonly Guid onPaymentStateTypeId = new Guid("{3DB4DB00-3A7F-4228-A9A3-A413B85C18B4}"); //Оформлен


            private static readonly Guid bankId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid applicantId = new Guid("{6F5B8A06-361E-4559-8A53-9CB480A9B16C}");
            private static readonly decimal postStaticPercent = 1.3m;

            private static List<Guid> benefitPaymentList = new List<Guid>
            {
                retirementBenefitPaymentId,childBenefitPaymentId,invalidBenefitPaymentId,survivorBenefitPaymentId,
                aidsFromBenefitPaymentId,aidsBenefitPaymentId,orphanBenefitPaymentId
            };
            public static List<ReportItem> Execute(WorkflowContext context, DateTime fd, DateTime ld)
            {
                var itemsBank = CalcBank(context, fd, ld);
                var itemsPost = CalcPost(context, fd, ld);
                foreach (var item in itemsBank)
                {
                    var itemPost = GetReportItem(itemsPost, item.orgName);
                    item.countAllForeignCitizen += itemPost.countAllForeignCitizen;
                    item.countUBKPerson += itemPost.countUBKPerson;
                    item.averageAmountUBK = (item.averageAmountUBK + itemPost.averageAmountUBK) / 2;
                    item.averageSumUBKTIAS = (item.averageSumUBKTIAS + itemPost.averageSumUBKTIAS) / 2;
                    item.countMSBFamily += itemPost.countMSBFamily;
                    item.averageSumMSBTIAS = (item.averageSumMSBTIAS + itemPost.averageSumMSBTIAS) / 2;
                    item.averageAmountMSB = (item.averageAmountMSB + itemPost.averageAmountMSB) / 2;
                    item.Uzbekistan += itemPost.Uzbekistan;
                    item.Tajikistan += itemPost.Tajikistan;
                    item.Belarus += itemPost.Belarus;
                    item.Armenia += itemPost.Armenia;
                    item.Moldova += itemPost.Moldova;
                    item.Kazakhstan += itemPost.Kazakhstan;
                    item.Russia += itemPost.Russia;
                    item.Ukraine += itemPost.Ukraine;
                    item.Others += itemPost.Others;
                }
                return itemsBank;
            }

            public static List<ReportItem> CalcPost(WorkflowContext context, DateTime fd, DateTime ld)
            {
                var items = new List<ReportItem>();
                var qb = new QueryBuilder(postOrderDefId, context.UserId);
                qb.Where("Application").Include("PaymentType")
                    .In(new object[]
                    {
                    uyBulogoKomokPaymentId,
                    retirementBenefitPaymentId,
                    childBenefitPaymentId,
                    survivorBenefitPaymentId,
                    aidsFromBenefitPaymentId,
                    aidsBenefitPaymentId,
                    orphanBenefitPaymentId
                    })
                    .And("&State")
                    .In(new object[]
                    {
                        approvedStateId,
                        onApprovingStateId,
                        onRegisteringStateId,
                        onPaymentStateTypeId
                    })
                    .And("RegDate").Ge(fd).And("RegDate").Le(ld);
                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var applicationSrc = query.JoinSource(query.Source, appDefId, SqlSourceJoinType.Inner, "Application");
                var applicantSrc = query.JoinSource(applicationSrc, applicantId, SqlSourceJoinType.Inner, "Applicant");
                query.AddAttribute(applicationSrc, "&Id");
                query.AddAttribute("&OrgId");
                query.AddAttribute(applicationSrc, "PaymentType");
                query.AddAttribute(applicationSrc, "PaymentSum");
                query.AddAttribute(applicationSrc, "DependentCount");
                query.AddAttribute(applicantSrc, "Citizenship");

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        var appId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                        var orgId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                        var paymentId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                        var amount = reader.IsDbNull(3) ? 0m : reader.GetDecimal(3);
                        var countUBKPerson = reader.IsDbNull(4) ? 0 : reader.GetInt32(4);
                        var citizenshipId = reader.IsDbNull(5) ? Guid.Empty : reader.GetGuid(5);
                        if (orgId == Guid.Empty) continue;
                        var orgName = context.Orgs.GetOrgName(orgId);
                        var item = GetReportItem(items, orgName);
                        if (paymentId.Equals(uyBulogoKomokPaymentId))
                        {
                            item.countUBKPerson += countUBKPerson;
                            item.countUBKFamily += 1;
                            item.averageAmountUBK += amount;
                            item.averageSumUBKTIAS += (amount + Math.Round((postStaticPercent * amount) / 100, 2));
                        }
                        if (benefitPaymentList.Contains(paymentId))
                        {
                            item.countMSBFamily += 1;
                            item.averageAmountMSB += amount;
                            item.averageSumMSBTIAS += (amount + Math.Round((postStaticPercent * amount) / 100, 2));
                        }
                        if (citizenshipId.Equals(new Guid("{70A7AFF3-D6AE-4A9E-92B7-C850487DBFB2}")))
                            item.Uzbekistan += 1;
                        if (citizenshipId.Equals(new Guid("{DDD66E3B-4731-41D0-99CD-5C13C4B1DFAB}")))
                            item.Tajikistan += 1;
                        if (citizenshipId.Equals(new Guid("{0DDD3EFE-67B3-4D58-9A85-26EF0F68C9B8}")))
                            item.Belarus += 1;
                        if (citizenshipId.Equals(new Guid("{4A4FF97B-EFE0-4CAB-97B6-BAD9F7E8E6F6}")))
                            item.Armenia += 1;
                        if (citizenshipId.Equals(new Guid("{89294ABB-88D9-432A-AFBE-CC7FAF63A77A}")))
                            item.Moldova += 1;
                        if (citizenshipId.Equals(new Guid("{6D47C8D9-BD38-45BC-83C2-789D2D20FB31}")))
                            item.Kazakhstan += 1;
                        if (citizenshipId.Equals(new Guid("{70F5BD8E-9CF8-412D-BE52-7284EB2888E2}")))
                            item.Russia += 1;
                        if (citizenshipId.Equals(new Guid("{D9924C3B-6081-4487-9857-0AD2C0A4F9EE}")))
                            item.Ukraine += 1;
                        if (citizenshipId.Equals(new Guid("{EA91668C-9C5B-49BB-BCAD-3FB329766C8A}")))
                            item.Others += 1;
                    }
                }
                foreach (var item in items)
                {
                    item.countAllForeignCitizen = item.Uzbekistan + item.Tajikistan + item.Belarus + item.Armenia + item.Moldova + item.Kazakhstan + item.Russia + item.Ukraine + item.Others;
                    var countMSBFamily = 0;
                    var countUBKFamily = 0;
                    if (item.countMSBFamily == 0) countMSBFamily = 1;
                    else countMSBFamily = item.countMSBFamily;
                    if (item.countUBKFamily == 0) countUBKFamily = 1;
                    else countUBKFamily = item.countUBKFamily;
                    item.averageAmountMSB = Math.Round(item.averageAmountMSB / countMSBFamily, 2);
                    item.averageAmountUBK = Math.Round(item.averageAmountUBK / countUBKFamily, 2);
                }
                return items;
            }


            public static List<ReportItem> CalcBank(WorkflowContext context, DateTime fd, DateTime ld)
            {
                var items = new List<ReportItem>();
                var qb = new QueryBuilder(bankOrderDefId, context.UserId);
                qb.Where("Application").Include("PaymentType")
                    .In(new object[]
                    {
                    uyBulogoKomokPaymentId,
                    retirementBenefitPaymentId,
                    childBenefitPaymentId,
                    survivorBenefitPaymentId,
                    aidsFromBenefitPaymentId,
                    aidsBenefitPaymentId,
                    orphanBenefitPaymentId
                    })
                    .And("&State")
                    .In(new object[]
                    {
                        approvedStateId,
                        onApprovingStateId,
                        onRegisteringStateId,
                        onPaymentStateTypeId
                    })
                    .And("RegDate").Ge(fd).And("RegDate").Le(ld);
                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var applicationSrc = query.JoinSource(query.Source, appDefId, SqlSourceJoinType.Inner, "Application");
                var bankSrc = query.JoinSource(applicationSrc, bankId, SqlSourceJoinType.LeftOuter, "Bank");
                var applicantSrc = query.JoinSource(applicationSrc, applicantId, SqlSourceJoinType.Inner, "Applicant");
                query.AddAttribute(applicationSrc, "&Id");
                query.AddAttribute("&OrgId");
                query.AddAttribute(applicationSrc, "PaymentType");
                query.AddAttribute(bankSrc, "Percent");
                query.AddAttribute(applicationSrc, "PaymentSum");
                query.AddAttribute(applicationSrc, "DependentCount");
                query.AddAttribute(applicantSrc, "Citizenship");

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        //var appId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                        var orgId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                        var paymentId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                        var percentOfBank = reader.IsDbNull(3) ? 0f : reader.GetDouble(3);

                        var amount = reader.IsDbNull(4) ? 0m : reader.GetDecimal(4);
                        var countUBKPerson = reader.IsDbNull(5) ? 0 : reader.GetInt32(5);
                        var citizenshipId = reader.IsDbNull(6) ? Guid.Empty : reader.GetGuid(6);
                        if (orgId == Guid.Empty) continue;
                        var orgName = context.Orgs.GetOrgName(orgId);
                        var item = GetReportItem(items, orgName);
                        if (paymentId.Equals(uyBulogoKomokPaymentId))
                        {
                            item.countUBKPerson += countUBKPerson;
                            item.countUBKFamily += 1;
                            item.averageAmountUBK += amount;
                            if (percentOfBank > 0)
                            {
                                item.averageSumUBKTIAS += (amount + Math.Round((Convert.ToDecimal(percentOfBank) * Convert.ToDecimal(amount)) / 100, 2));
                            }
                            else
                            {
                                item.averageSumUBKTIAS += (amount + Math.Round((postStaticPercent * amount) / 100, 2));
                            }
                        }
                        if (benefitPaymentList.Contains(paymentId))
                        {
                            item.countMSBFamily += 1;
                            item.averageAmountMSB += amount;
                            if (percentOfBank > 0)
                            {
                                item.averageSumMSBTIAS += (amount + Math.Round((Convert.ToDecimal(percentOfBank) * Convert.ToDecimal(amount)) / 100, 2));
                            }
                            else
                            {
                                item.averageSumMSBTIAS += (amount + Math.Round((postStaticPercent * amount) / 100, 2));
                            }
                        }
                        if (citizenshipId.Equals(new Guid("{70A7AFF3-D6AE-4A9E-92B7-C850487DBFB2}")))
                            item.Uzbekistan += 1;
                        if (citizenshipId.Equals(new Guid("{DDD66E3B-4731-41D0-99CD-5C13C4B1DFAB}")))
                            item.Tajikistan += 1;
                        if (citizenshipId.Equals(new Guid("{0DDD3EFE-67B3-4D58-9A85-26EF0F68C9B8}")))
                            item.Belarus += 1;
                        if (citizenshipId.Equals(new Guid("{4A4FF97B-EFE0-4CAB-97B6-BAD9F7E8E6F6}")))
                            item.Armenia += 1;
                        if (citizenshipId.Equals(new Guid("{89294ABB-88D9-432A-AFBE-CC7FAF63A77A}")))
                            item.Moldova += 1;
                        if (citizenshipId.Equals(new Guid("{6D47C8D9-BD38-45BC-83C2-789D2D20FB31}")))
                            item.Kazakhstan += 1;
                        if (citizenshipId.Equals(new Guid("{70F5BD8E-9CF8-412D-BE52-7284EB2888E2}")))
                            item.Russia += 1;
                        if (citizenshipId.Equals(new Guid("{D9924C3B-6081-4487-9857-0AD2C0A4F9EE}")))
                            item.Ukraine += 1;
                        if (citizenshipId.Equals(new Guid("{EA91668C-9C5B-49BB-BCAD-3FB329766C8A}")))
                            item.Others += 1;
                    }
                }
                foreach (var item in items)
                {
                    item.countAllForeignCitizen = item.Uzbekistan + item.Tajikistan + item.Belarus + item.Armenia + item.Moldova + item.Kazakhstan + item.Russia + item.Ukraine + item.Others;
                    var countMSBFamily = 0;
                    var countUBKFamily = 0;
                    if (item.countMSBFamily == 0) countMSBFamily = 1;
                    else countMSBFamily = item.countMSBFamily;
                    if (item.countUBKFamily == 0) countUBKFamily = 1;
                    else countUBKFamily = item.countUBKFamily;
                    item.averageAmountMSB = Math.Round(item.averageAmountMSB / countMSBFamily, 2);
                    item.averageAmountUBK = Math.Round(item.averageAmountUBK / countUBKFamily, 2);
                }
                return items;
            }

            static ReportItem GetReportItem(List<ReportItem> items, string orgName)
            {
                var item = items.FirstOrDefault(x => x.orgName == orgName);
                if (item != null) return item;
                item = new ReportItem
                {
                    orgName = orgName,
                    rowNo = items.Count > 0 ? (items.Max(x => x.rowNo) + 1) : 1
                };
                items.Add(item);
                return item;
            }
            public class ReportItem
            {
                public int rowNo { get; set; }
                public string orgName { get; set; }
                public int countAllForeignCitizen { get; set; }
                public int countUBKFamily { get; set; }
                public int countUBKPerson { get; set; }
                public decimal averageAmountUBK { get; set; } //UBK-Uy bulogo komok
                public decimal averageSumUBKTIAS { get; set; } // TIAS-taking into account services
                public int countMSBFamily { get; set; } //MSB-monthly social benefit
                public decimal averageAmountMSB { get; set; }
                public decimal averageSumMSBTIAS { get; set; }
                public int Uzbekistan { get; set; }
                public int Tajikistan { get; set; }
                public int Belarus { get; set; }
                public int Armenia { get; set; }
                public int Moldova { get; set; }
                public int Kazakhstan { get; set; }
                public int Russia { get; set; }
                public int Ukraine { get; set; }
                public int Others { get; set; }
            }
        }
        public static class Report_1021
        {
            private static readonly Guid uyBulogoKomokPaymentId = new Guid("{3193A90D-380B-4428-B7B1-04C548AA902E}");

            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{1A09ECD6-55E1-4307-862E-6F98F47E252C}");
            private static readonly Guid reportItemDefId = new Guid("{2A8709AB-3522-4019-A29F-5C333893645B}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");

            private static readonly Guid retirementBenefitPaymentId = new Guid("{AFAA7F86-74AE-4260-9933-A56F7845E55A}");  // ЕСП престарелым гражданам
            private static readonly Guid childBenefitPaymentId = new Guid("{AB3F8C41-897A-4574-BAA0-B7CD4AAA1C80}");  // ЕСП на инвалида - члена семьи
            private static readonly Guid invalidBenefitPaymentId = new Guid("{70C28E62-2387-4A59-917D-A366ADE119A8}");  // ЕСП по инвалидности
            private static readonly Guid survivorBenefitPaymentId = new Guid("{839D5712-E75B-4E71-83F7-168CE4F089C0}");  // ЕСП детям при утере кормильца 
            private static readonly Guid aidsFromBenefitPaymentId = new Guid("{3BEBE4F9-0B15-41CB-9B96-54E83819AB0F}");  // ЕСП детям, инфецированным ВИЧ/СПИД
            private static readonly Guid aidsBenefitPaymentId = new Guid("{47EEBFBC-A4E9-495D-A6A1-F87B5C3057C9}");  // ЕСП детям от матерей с ВИЧ/СПИД до 18 месяцев
            private static readonly Guid orphanBenefitPaymentId = new Guid("{4F12C366-7E2F-4208-9CB8-4EAB6E6C0EF1}");   // ЕСП круглым сиротам 

            // States                               
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid canceledStateId = new Guid("{C65E63CC-54E9-424E-AFDC-5A8DB1CB04A3}"); // Аннулирован
            private static readonly Guid onApprovingStateId = new Guid("{5CD9E88D-671E-4A44-AD92-9F74DA3B47F7}"); //На утверждении
            private static readonly Guid onRegisteringStateId = new Guid("{A9CD37C4-A718-4DE1-9E95-EC8EC280C8D4}"); // На регистрации
            private static readonly Guid refusedStateId = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}"); // Отказан
            private static readonly Guid onPaymentStateTypeId = new Guid("{3DB4DB00-3A7F-4228-A9A3-A413B85C18B4}"); //Оформлен


            private static readonly Guid bankId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid applicantId = new Guid("{6F5B8A06-361E-4559-8A53-9CB480A9B16C}");
            private static readonly Guid statusOfRefugee = new Guid("{2E815185-6075-4207-8271-541970BDC3EF}");
            // new Guid("{C7682621-4931-4125-8F0D-F568295B668B}");
            private static readonly decimal postStaticPercent = 1.3m;

            private static List<Guid> benefitPaymentList = new List<Guid>
            {
                retirementBenefitPaymentId,childBenefitPaymentId,invalidBenefitPaymentId,survivorBenefitPaymentId,
                aidsFromBenefitPaymentId,aidsBenefitPaymentId,orphanBenefitPaymentId
            };

            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {
                var items = new List<ReportItem>();
                var fd = new DateTime(year, month, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var qb = new QueryBuilder(appDefId, context.UserId);
                qb.Where("PaymentType")
                    .In(new object[]
                    {
                    uyBulogoKomokPaymentId,
                    retirementBenefitPaymentId,
                    childBenefitPaymentId,
                    survivorBenefitPaymentId,
                    aidsFromBenefitPaymentId,
                    aidsBenefitPaymentId,
                    orphanBenefitPaymentId
                    })
                    .And("&State")
                    .In(new object[]
                    {
                        approvedStateId,
                        onApprovingStateId,
                        onRegisteringStateId,
                        onPaymentStateTypeId
                    }).And("Applicant").Include("DocumentType").Eq(statusOfRefugee).End()
                        .And("RegDate").Ge(fd).And("RegDate").Le(ld);
                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var bankSrc = query.JoinSource(query.Source, bankId, SqlSourceJoinType.LeftOuter, "Bank");
                var applicantSrc = query.JoinSource(query.Source, applicantId, SqlSourceJoinType.Inner, "Applicant");
                query.AddAttribute("&Id");
                query.AddAttribute("&OrgId");
                query.AddAttribute("PaymentType");
                query.AddAttribute(bankSrc, "Percent");
                query.AddAttribute("PaymentSum");
                query.AddAttribute("DependentCount");
                query.AddAttribute(applicantSrc, "Citizenship");


                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        //if (bankSrc == Guid.Empty) continue;
                        var appId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                        var orgId = reader.IsDbNull(1) ? Guid.Empty : reader.GetGuid(1);
                        var paymentId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                        var percentOfBank = reader.IsDbNull(3) ? 0f : reader.GetDouble(3);
                        var amount = reader.IsDbNull(4) ? 0m : reader.GetDecimal(4);
                        var countUBKPerson = reader.IsDbNull(5) ? 0 : reader.GetInt32(5);
                        var citizenshipId = reader.IsDbNull(6) ? Guid.Empty : reader.GetGuid(6);


                        if (orgId == Guid.Empty) continue;
                        var orgName = context.Orgs.GetOrgName(orgId);
                        var item = GetReportItem(items, orgName);

                        if (paymentId.Equals(uyBulogoKomokPaymentId))
                        {
                            item.countUBKPerson += countUBKPerson;
                            item.countUBKFamily += 1;
                            item.averageAmountUBK += amount;
                            if (percentOfBank > 0)
                            {
                                item.averageSumUBKTIAS += (amount + Math.Round((Convert.ToDecimal(percentOfBank) * Convert.ToDecimal(amount)) / 100, 2));
                            }
                            else
                            {
                                item.averageSumUBKTIAS += (amount + Math.Round((postStaticPercent * amount) / 100, 2));
                            }
                        }
                        if (benefitPaymentList.Contains(paymentId))
                        {
                            item.countMSBFamily += 1;
                            item.averageAmountMSB += amount;
                            if (percentOfBank > 0)
                            {
                                item.averageSumMSBTIAS += (amount + Math.Round((Convert.ToDecimal(percentOfBank) * Convert.ToDecimal(amount)) / 100, 2));
                            }
                            else
                            {
                                item.averageSumMSBTIAS += (amount + Math.Round((postStaticPercent * amount) / 100, 2));
                            }
                        }

                        if (citizenshipId.Equals(new Guid("{70A7AFF3-D6AE-4A9E-92B7-C850487DBFB2}")))
                            item.Uzbekistan += 1;
                        if (citizenshipId.Equals(new Guid("{DDD66E3B-4731-41D0-99CD-5C13C4B1DFAB}")))
                            item.Tajikistan += 1;
                        if (citizenshipId.Equals(new Guid("{3521D5A1-448F-4FD2-BD5B-B191F05F1262}")))
                            item.China += 1;
                        if (citizenshipId.Equals(new Guid("{E598DFA1-E503-4F3C-8DEC-D185F58F548F}")))
                            item.Turkey += 1;
                        if (citizenshipId.Equals(new Guid("{049FA841-4D56-4936-989D-501C54D32F92}")))
                            item.Pakistan += 1;
                        if (citizenshipId.Equals(new Guid("{6D47C8D9-BD38-45BC-83C2-789D2D20FB31}")))
                            item.Kazakhstan += 1;
                        if (citizenshipId.Equals(new Guid("{70F5BD8E-9CF8-412D-BE52-7284EB2888E2}")))
                            item.Russia += 1;
                        if (citizenshipId.Equals(new Guid("{EA91668C-9C5B-49BB-BCAD-3FB329766C8A}")))
                            item.Others += 1;

                    }
                }

                foreach (var item in items)
                {
                    item.countAllForeignCitizen = item.Uzbekistan + item.Tajikistan + item.China + item.Turkey + item.Pakistan + item.Kazakhstan + item.Russia + item.Others;
                    var countMSBFamily = 0;
                    var countUBKFamily = 0;
                    if (item.countMSBFamily == 0) countMSBFamily = 1;
                    else countMSBFamily = item.countMSBFamily;
                    if (item.countUBKFamily == 0) countUBKFamily = 1;
                    else countUBKFamily = item.countUBKFamily;
                    item.averageAmountMSB = Math.Round(item.averageAmountMSB / countMSBFamily, 2);
                    item.averageAmountUBK = Math.Round(item.averageAmountUBK / countUBKFamily, 2);
                }
                return items;
            }

            static ReportItem GetReportItem(List<ReportItem> items, string orgName)
            {
                var item = items.FirstOrDefault(x => x.orgName == orgName);
                if (item != null) return item;
                item = new ReportItem
                {
                    orgName = orgName,
                    rowNo = items.Count > 0 ? (items.Max(x => x.rowNo) + 1) : 1
                };
                items.Add(item);
                return item;
            }
            public class ReportItem
            {
                public int rowNo { get; set; }
                public string orgName { get; set; }
                public int countAllForeignCitizen { get; set; }
                public int countUBKFamily { get; set; }
                public int countUBKPerson { get; set; }
                public decimal averageAmountUBK { get; set; } //UBK-Uy bulogo komok
                public decimal averageSumUBKTIAS { get; set; } // TIAS-taking into account services
                public int countMSBFamily { get; set; } //MSB-monthly social benefit
                public decimal averageAmountMSB { get; set; }
                public decimal averageSumMSBTIAS { get; set; }
                public int Uzbekistan { get; set; }
                public int Tajikistan { get; set; }
                public int China { get; set; }
                public int Turkey { get; set; }
                public int Pakistan { get; set; }
                public int Kazakhstan { get; set; }
                public int Russia { get; set; }
                public int Others { get; set; }
            }

        }

        public static class PatientCountOfMonth_6001
        {
            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {
                CardsDays = new Dictionary<string, int>();
                return Build(context, year, month, context.UserId, context.GetUserInfo());
            }
            /*-------------------------------------Documents----------------------------------------------------------------------*/
            public static readonly Guid cardDefId = new Guid("{683B63E2-B0C6-470F-8A01-5D73AB145F8A}"); // PatientCard
            public static readonly Guid procedurDefId = new Guid("{79E864E5-7810-46F8-A2C2-E4C92B1D3290}");
            public static readonly Guid iprDefId = new Guid("{A1872B7D-0408-4338-B20D-42E4BDAEC2DA}");
            public static readonly Guid ReportDocId = new Guid("{A1C8DF40-EDCB-4DFF-AD77-F0A03D5B0C90}");
            public static readonly Guid ReportItemDocId = new Guid("{7033EA4A-C8AB-4A5B-BBDD-62143D864AD6}");
            private static readonly Guid WeekReportDefId = new Guid("{E71277B2-63E8-4F39-9771-92FDC580618B}");
            public static readonly Guid ApprovedDocStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}");
            private static readonly Guid ApplicantDefId = new Guid("{6F5B8A06-361E-4559-8A53-9CB480A9B16C}");
            private static readonly Guid vypiskaDefId = new Guid("{DA9CBB90-6527-41DA-A25E-690EFE24AE8A}");
            /*--------------------------------------------------------------------------------------------------------------------*/
            private static readonly Guid field1 = new Guid("{D402B05E-6906-4401-BCB1-35760C8F1CBF}");
            private static readonly Guid field2 = new Guid("{CA4AF527-67AE-4C49-8600-B8F84EAEB939}");
            private static readonly Guid field3 = new Guid("{9DF7929C-3016-4584-ABC8-5A03A714F148}");
            private static readonly Guid field4 = new Guid("{527F2FB1-EBCE-4BB7-92A9-699938B73A05}");
            private static readonly Guid field5 = new Guid("{EA23E687-7AF5-47D6-88C3-89D95C1D2773}");
            private static readonly Guid field6 = new Guid("{04724A1C-8842-4F09-BE51-AD6784B128EF}");
            private static readonly Guid field7 = new Guid("{6CE71B14-B8CB-411E-A954-2B16F6CC1A55}");
            private static readonly Guid field8 = new Guid("{98A4304F-2C33-40AB-A5CE-5EF9A353C7DC}");
            private static readonly Guid field9 = new Guid("{38872A87-3D1C-4F43-855E-7FEE6D4B1434}");
            private static readonly Guid field10 = new Guid("{75262FBD-6CA4-4109-8800-552C91C826A4}");
            private static readonly Guid field11 = new Guid("{91A6B5B6-38B0-4BA0-9DD6-FC633635B7F9}");
            private static readonly Guid field11_1 = new Guid("{1EFB8855-6C2A-4B25-8C00-9236CC393FE7}");
            private static readonly Guid field12 = new Guid("{A1CECF79-C5E4-4E7D-BDF6-72C37F796B33}");
            private static readonly Guid field13 = new Guid("{AEE289F4-D7D0-4FD9-82A7-3186DC047CA0}");
            private static readonly Guid field14 = new Guid("{383659E1-22D3-4703-9A3A-49BEA13D7645}");
            private static readonly Guid field15 = new Guid("{3517C817-DE1A-4D2F-93E1-EF34B022F904}");
            private static readonly Guid field16 = new Guid("{E11D16AE-367F-487C-8D12-F802BDFBCC38}");
            private static readonly Guid field17 = new Guid("{D3B6156D-9B0C-4D8B-9293-0FBC044F3642}");
            private static readonly Guid field18 = new Guid("{12CA4E09-0970-4B3C-87EE-25446B09F120}");
            private static readonly Guid field19 = new Guid("{E5313C2B-8571-4F1D-972B-152190FE7462}");
            private static readonly Guid field20 = new Guid("{BA504F1D-AC4A-44E8-894A-C677BF132383}");
            private static readonly Guid field21 = new Guid("{1F67482C-986C-4776-B299-C57A3CB6F9F4}");
            private static readonly Guid field22 = new Guid("{ED50C0C5-F6E8-4CA6-BEE8-6F71CBB3F3C9}");
            private static readonly Guid field23 = new Guid("{504886C9-D4A4-4198-84C2-E316E1E195CF}");
            private static readonly Guid field24 = new Guid("{F66E3355-FE3B-4C5F-8E78-E771DBCCB170}");
            private static readonly Guid field24_1 = new Guid("{4D4ACC4C-2CFD-4F60-9915-72E36D5EB45E}");  //ЭКГ
            private static readonly Guid field24_2 = new Guid("{473F4C3C-8AB9-4880-AC9B-BBB1F9B6AA05}");  //УЗДГ
            private static readonly Guid field24_3 = new Guid("{A6BE6905-2105-4E6F-99A6-1D05D4852AA8}");  //УЗИ 
            private static readonly Guid field25 = new Guid("{896796C6-BA76-404D-8B45-8DEF98FABDED}");
            private static readonly Guid field26 = new Guid("{B0477D17-2F03-4E95-9FC5-A4453F3E3A8E}");
            private static readonly Guid field27 = new Guid("{D982C076-C8B9-455D-A6D3-34F7ECB1DEAC}");
            private static readonly Guid field28 = new Guid("{B726E884-E163-4138-8A09-1A5E1E9383E9}");
            private static readonly Guid field29 = new Guid("{4B4AEA6C-3DBC-4596-AB03-A01027214D95}");
            private static readonly Guid field30 = new Guid("{F00753F2-1769-4C31-895F-3E5E5EBE2AE3}");
            private static readonly Guid field31 = new Guid("{15ADAEBD-6036-4108-9E7E-55AE491A9D89}");
            private static readonly Guid field32 = new Guid("{CCA0390E-A9CE-4AA7-A2DC-23F851E8D00D}");
            private static readonly Guid field32_1 = new Guid("{368607B0-3E6A-4426-9273-F082A90F0F5E}");
            private static readonly Guid field33 = new Guid("{DFC699BA-6F04-4994-91B6-4D767AA7B171}");
            private static readonly Guid field34 = new Guid("{89A90F92-2C2D-471F-8E45-BAAEC673094D}");
            private static readonly Guid field35 = new Guid("{0D6ADA32-5961-4630-A7D6-EC4A7BEDC287}");
            private static readonly Guid field36 = new Guid("{E382801C-E32D-43BF-B933-B12415F5C9EE}");
            private static readonly Guid field37 = new Guid("{A1A7455A-461B-461A-AFE7-D941189A984B}");
            private static readonly Guid field38 = new Guid("{DEA1DA30-D199-4429-9C40-85650881B16F}");
            private static readonly Guid field39 = new Guid("{0123CFF0-D8D5-4950-88C8-4CD816AFBBFF}");

            private static readonly Guid field40 = new Guid("{5EF451AE-91E0-4EBA-93E8-6DA84A418C07}");
            private static readonly Guid field40_1 = new Guid("{641366ED-AF6B-4542-9EEA-27D89961A396}");
            private static readonly Guid field41 = new Guid("{4C7A382F-2F3F-4F26-A555-9177130D4DDC}");
            private static readonly Guid field42 = new Guid("{11358616-5BE7-415D-A513-A11F4123851F}");
            private static readonly Guid field43 = new Guid("{55DCE035-FD2A-493C-9CD6-06EA6B7F389A}");
            //районы  
            private static readonly Guid field44 = new Guid("{F9C2EC34-2432-4512-9647-243A87CFD52E}");
            private static readonly Guid field45 = new Guid("{F54A1E9E-068B-46EF-9D75-FB6C39654FEB}");
            private static readonly Guid field46 = new Guid("{7F5F45D0-E179-42C6-A3EA-30FA107551F2}");
            private static readonly Guid field47 = new Guid("{81D925CF-0B63-4E05-AFD3-859DB982CF3A}");
            private static readonly Guid field48 = new Guid("{686B8E10-B535-4181-9246-B899A9598DEE}");
            private static readonly Guid field49 = new Guid("{3C0DEE8D-7F40-4EB4-8FDC-8456FBFC8E4C}");
            private static readonly Guid field50 = new Guid("{AF21CE80-2DC7-4958-9336-8BD1442CF5CF}");
            private static readonly Guid field51 = new Guid("{4E2B96A8-A5DD-44C1-8369-32BCBE18E61C}");
            private static readonly Guid field52 = new Guid("{91B58917-8D9E-4922-AADF-5BE9BA5D9FE8}");
            private static readonly Guid field53 = new Guid("{6C3ABA2D-5CD1-4E2C-BB0B-BC55ABDCFAF4}");
            private static readonly Guid field54 = new Guid("{56AD8CD7-641B-4E73-ADFC-F93A1C8F63CD}");
            private static readonly Guid field55 = new Guid("{ED3D5A89-9696-4B0C-AB58-F764C0332B76}");
            private static readonly Guid field56 = new Guid("{A5766D54-CB05-43EB-B3C1-CA42A1176A3A}");
            private static readonly Guid field57 = new Guid("{A7AF1299-988E-4836-84CF-6545B931E94F}");
            private static readonly Guid field58 = new Guid("{54BCFC63-926D-49B5-88D1-B3ABE7B341D6}");
            private static readonly Guid field59 = new Guid("{1CA5F9B0-E382-41FB-B654-F6DD7D8DCB54}");
            private static readonly Guid field60 = new Guid("{7A70FDD5-6123-4A90-8FCB-FBEDC7704955}");
            private static readonly Guid field61 = new Guid("{D99E0EDF-633E-45D0-B879-D613B21C9C75}");
            private static readonly Guid field62 = new Guid("{E6564BFF-B8DA-40EF-B91D-C66CC021D786}");
            private static readonly Guid field63 = new Guid("{9808D969-B314-4B4C-8D89-FE7F470195A1}");
            private static readonly Guid field64 = new Guid("{A7820FFA-070A-41AF-8C66-B308251ACB85}");
            private static readonly Guid field65 = new Guid("{EEA75CE5-4299-4DCE-9CA3-1E0DAD642043}");
            private static readonly Guid field66 = new Guid("{B81A244A-3658-444C-AB27-788D9179D53C}");
            private static readonly Guid field67 = new Guid("{C6949E8C-297C-4F78-8FB2-DDF9AD406A15}");
            private static readonly Guid field68 = new Guid("{9E9F2D55-E0E4-4231-8D41-166480986EB8}");
            private static readonly Guid field69 = new Guid("{128C8EAC-FE3D-47F6-B6C0-82A069CC6DB9}");
            private static readonly Guid field70 = new Guid("{9CCD848C-6F45-4D4D-A58F-F50C610BB6E7}");
            private static readonly Guid field71 = new Guid("{5128C983-DEAD-4657-AEA3-795176052200}");
            private static readonly Guid field72 = new Guid("{63DB2834-DBBC-48FE-9D42-1F39C5769A40}");
            private static readonly Guid field73 = new Guid("{F58A1BC2-BD6A-4A18-A88A-F9FB345D317D}");
            private static readonly Guid field74 = new Guid("{D7C0D83B-7038-4CE3-8D2A-038A7FC3A295}");
            private static readonly Guid field75 = new Guid("{999CB7F2-4751-4A32-862D-AA72140700A2}");
            private static readonly Guid field76 = new Guid("{93662031-C385-45F4-8881-BDD0838CE1E6}");
            private static readonly Guid field77 = new Guid("{23A12B87-12E3-4FA3-AF43-88476ABB5415}");
            private static readonly Guid field78 = new Guid("{7D243200-B0DD-4086-B97F-84AC1340A078}");
            private static readonly Guid field79 = new Guid("{14B59933-9FD1-4D20-9344-85734ADB72F4}");
            private static readonly Guid field80 = new Guid("{64CE0043-853F-418E-85E9-633D4095B201}");
            private static readonly Guid field81 = new Guid("{15ABE2D9-4CD6-449A-AD48-B177F78612FD}");
            private static readonly Guid field82 = new Guid("{57A8C4F1-3DC6-44FB-8B7C-FA5288D2EDFD}");
            private static readonly Guid field83 = new Guid("{EA5A9498-1708-4327-8696-8E804474DF79}");
            private static readonly Guid field84 = new Guid("{BBB9B6B8-B297-4C03-8EAB-FBF632769059}");
            private static readonly Guid field85 = new Guid("{5F1280B2-5191-4237-A13D-3949F0CB3719}");
            private static readonly Guid field86 = new Guid("{E8420F21-86E5-48F4-82C3-333F0E8C5697}");
            private static readonly Guid field87 = new Guid("{71D66396-B1AA-4B00-B0F0-A6C4B53C52A5}");
            private static readonly Guid field88 = new Guid("{E8F6BBA0-D400-4890-A879-E6AB6DB2E601}");
            private static readonly Guid field89 = new Guid("{DEAA9659-D7CC-42ED-8331-699E023B3E60}");
            private static readonly Guid field90 = new Guid("{2F4C0B06-BD30-450A-B403-64F3F0F873D6}");
            private static readonly Guid field91 = new Guid("{0CDEBAC7-AA1A-4094-BC86-401ED15D25FB}");
            private static readonly Guid field92 = new Guid("{6AC71F33-E93E-44AA-BB49-351CD9B3A558}");
            private static readonly Guid field93 = new Guid("{96F11B25-09F0-4AA5-A86D-97996646CF83}");
            private static readonly Guid field94 = new Guid("{DAABDE19-42B3-4757-8C4F-B0E1DADD489E}");
            private static readonly Guid field95 = new Guid("{E8965B60-CCD3-4EC8-BE11-DAD269A68D82}");
            private static readonly Guid field96 = new Guid("{350B63B0-016E-4445-ADD1-D41D7F7A8D35}");
            private static readonly Guid field97 = new Guid("{5C03BEDE-6D2F-499D-9305-D7265499CD6F}");
            private static readonly Guid field98 = new Guid("{F06A9AE9-51C4-4897-A036-E0A71DEDAADE}");
            private static readonly Guid field99 = new Guid("{492AB488-3D34-4295-9603-952641DA881A}");
            private static readonly Guid field100 = new Guid("{610967C9-32F9-41E9-AAC5-EDDC3600E11E}");
            private static readonly Guid field101 = new Guid("{FAC0322D-51E8-47B5-BB4E-226E4906F3F4}");
            private static readonly Guid field102 = new Guid("{B1F68566-5086-4616-A917-942903EFCA77}");
            private static readonly Guid field103 = new Guid("{0529B123-301A-44EF-A47B-48581EE4B938}");
            private static readonly Guid field104 = new Guid("{0FB52D18-974F-47EF-A340-1DBE30870A9F}");
            private static readonly Guid field105 = new Guid("{F5403F54-89A5-4434-A648-1F4846849E40}");
            private static readonly Guid field106 = new Guid("{67557EBA-EDB3-4C39-9DF5-6CDDA027E0CB}");
            private static readonly Guid field107 = new Guid("{A131016F-346C-4ED4-8FE7-E708C8CFCE45}");
            private static readonly Guid field108 = new Guid("{C6DE46C8-7EC1-4E1A-8BEC-238DAC021857}");
            private static readonly Guid field109 = new Guid("{9DC3BCC1-88CD-46BF-9D10-3C12E28D6488}");
            private static readonly Guid field110 = new Guid("{D469E9A3-8518-446B-9DBF-20E83B715E39}");
            private static readonly Guid field111 = new Guid("{92628B3E-8D05-4BE7-97AD-D00FC09CEFE2}");
            private static readonly Guid field112 = new Guid("{9C46C759-1F7A-481A-A810-F365994721C1}");
            private static readonly Guid field113 = new Guid("{8E322A0F-9064-4A3B-B644-601C3E70C707}");
            private static readonly Guid field114 = new Guid("{F31338BF-1D35-44FC-AB3D-4DFEAD242C52}");
            private static readonly Guid field115 = new Guid("{EF25BA27-826A-4BAA-98B7-45E6105B38E0}");
            private static readonly Guid field116 = new Guid("{AC683072-CF8F-4BC7-B10D-05AC572539F2}");
            private static readonly Guid field117 = new Guid("{74C074E1-D508-4DA5-9E92-D4B7C135866A}");
            private static readonly Guid field118 = new Guid("{B2C8D37F-5361-464A-983A-65A48EEABBF5}");
            private static readonly Guid field119 = new Guid("{4A814B77-AD08-43EF-BD2E-8472B7D0DCBB}");
            private static readonly Guid field120 = new Guid("{B1A7EB0F-D9BE-4A6F-9BC5-96B9760C7B97}");
            private static readonly Guid field121 = new Guid("{0F43D627-8FD4-48B5-A894-AD734A482E7A}");
            private static readonly Guid field122 = new Guid("{6E159D42-88B0-42C8-A7F2-A06C2C096514}");
            private static readonly Guid field123 = new Guid("{75E686FA-F3DA-4501-BB86-D72DA819C208}");
            private static readonly Guid field124 = new Guid("{348A1E10-7D75-4134-BDE6-AFE5A8B0C816}");
            private static readonly Guid field125 = new Guid("{BF82748C-40D3-42CE-9538-A979E599D164}"); //Переходящие с предыдущей недели/месяц ЛОВЗ
            private static readonly Guid field126 = new Guid("{459B083A-D12A-4822-A30F-4685CA8F854C}"); //Сопровождающие[new]
            private static readonly Guid field127 = new Guid("{44922F13-56F2-4F95-987E-21C9693EA196}"); //Хороший результат
            private static readonly Guid field128 = new Guid("{8407F3CA-FDC8-4DD2-8444-2A303C524994}"); //Удовлетворительный ответ
            private static readonly Guid field129 = new Guid("{33EA46C5-36CA-4EBB-A7FB-7F55D702A846}"); //Без изменений

            // Areas
            private static readonly Guid region1 = new Guid("{FCB6DB01-469E-4254-9BB6-E3409057A779}");
            private static readonly Guid region2 = new Guid("{4386D93C-2DF6-4F99-A438-55C20447FD24}");
            private static readonly Guid region3 = new Guid("{F2662FF6-FFA7-4707-8284-3FB52436C1F1}");
            private static readonly Guid region4 = new Guid("{591191B0-7827-4C47-AC47-FB03C685E911}");
            private static readonly Guid region5 = new Guid("{65C856CD-69E5-4CA4-BB8C-8F382AE659E8}");
            private static readonly Guid region6 = new Guid("{D1067965-ACC3-4C43-9CA3-DC50C0E4D128}");
            private static readonly Guid region7 = new Guid("{4171C264-3F39-4B23-AAAC-B4282C68C6A6}");
            private static readonly Guid dis1 = new Guid("{79AAD00B-DC3A-4247-BCF7-CDE467B96E4F}");
            private static readonly Guid dis2 = new Guid("{F3D9FBB2-D7CF-4CF9-B085-7D0793B37370}");
            private static readonly Guid dis3 = new Guid("{6674AFF4-B55C-45E3-A487-249317B33DF4}");
            private static readonly Guid dis4 = new Guid("{CA401F89-C9F5-4CEF-A7CF-2A0FDFEDD5FF}");
            private static readonly Guid dis5 = new Guid("{729FD82B-B855-4925-BBA6-6D450889D3F9}");
            private static readonly Guid dis6 = new Guid("{42D826F0-4AAB-4F4F-B08D-191ADDF55743}");
            private static readonly Guid dis7 = new Guid("{5579EDE7-40E8-4092-A6CF-30D96803637C}");
            private static readonly Guid dis8 = new Guid("{C9F27576-91E7-4A99-856E-38BBA988AF22}");
            private static readonly Guid dis9 = new Guid("{767CB4C4-C05F-4229-8960-6604744FFF0F}");
            private static readonly Guid dis10 = new Guid("{D9E97D93-0CE2-421F-87A9-3FF1554D3BBC}");
            private static readonly Guid dis11 = new Guid("{7497BA2E-F5F0-4663-B337-2E8966D1A919}");
            private static readonly Guid dis12 = new Guid("{5208308E-2561-4CC2-8BB5-88584F615E71}");
            private static readonly Guid dis13 = new Guid("{03B2DB0C-12B7-44F8-AF9F-E51DA49F7D57}");
            private static readonly Guid dis14 = new Guid("{15895A37-560D-4AF0-A971-05EB7C19AD58}");
            private static readonly Guid dis15 = new Guid("{F8041D96-55D9-4840-B175-12CF10529AFF}");
            private static readonly Guid dis16 = new Guid("{8369FB92-2EB4-43E7-B9D1-AAD62A06D307}");
            private static readonly Guid dis17 = new Guid("{85706905-A4E4-4DED-973D-EC1C09776746}");
            private static readonly Guid dis18 = new Guid("{4DB17EAE-4381-4E0E-94CD-108E0F421E36}");
            private static readonly Guid dis19 = new Guid("{E28660FF-2877-472B-BA70-1A89F5E8A8E6}");
            private static readonly Guid dis20 = new Guid("{601E1AB1-FD5A-42DE-BE36-6FA265CE0C59}");
            private static readonly Guid dis21 = new Guid("{1E549247-6C19-4ED6-A013-E028B4EE0887}");
            private static readonly Guid dis22 = new Guid("{5C8C633B-125F-4CDF-902C-574CF78A6728}");
            private static readonly Guid dis23 = new Guid("{CB9A187F-4E16-41BC-A745-3DB9ADEB1B02}");
            private static readonly Guid dis24 = new Guid("{8B5C6DA6-4283-4D8B-AD37-153874EDF0AD}");
            private static readonly Guid dis25 = new Guid("{7D735007-FDB7-48EA-A9E8-988BE0D40C96}");
            private static readonly Guid dis26 = new Guid("{57FD5C59-1C04-478B-99AB-BB330DF8B2EE}");
            private static readonly Guid dis27 = new Guid("{8497A553-07EE-4146-86BA-ED714C8E2D2B}");
            private static readonly Guid dis28 = new Guid("{156E9D8B-008E-4A09-8142-96CEC1AEA1F7}");
            private static readonly Guid dis29 = new Guid("{5AC16B2B-9A3E-4797-9CDE-30C5B6875B2F}");
            private static readonly Guid dis30 = new Guid("{446AB760-4952-480A-A2C8-C6E55E530306}");
            private static readonly Guid dis31 = new Guid("{F523CE1A-E8AA-4C28-B54A-CDBB9FDA06EE}");
            private static readonly Guid dis32 = new Guid("{B83B35FC-8DE8-4531-A55B-2122B6927BB7}");
            private static readonly Guid dis33 = new Guid("{7241D184-10D5-44EA-9696-FB17CC54908D}");
            private static readonly Guid dis34 = new Guid("{0BB1ED87-390A-4E78-9C39-43548000050B}");
            private static readonly Guid dis35 = new Guid("{000A1444-070E-4E88-B721-A124286CDF52}");
            private static readonly Guid dis36 = new Guid("{C028DF94-6F08-427B-B7E3-144E288AC8DF}");
            private static readonly Guid dis37 = new Guid("{CDD62071-0103-4AEA-9716-F878A8EC794A}");
            private static readonly Guid dis38 = new Guid("{44AC559E-9176-471D-9625-B4F5A8D4C017}");
            private static readonly Guid dis39 = new Guid("{29DD2C55-E331-4BDF-BFFB-8BAF01CE38A4}");
            private static readonly Guid dis40 = new Guid("{7BB62C60-E8D6-4CA2-88A3-9449E5F15EE9}");
            private static readonly Guid dis41 = new Guid("{7EC8E15D-FADD-4973-8253-D7CE8A3F8B7A}");
            private static readonly Guid dis42 = new Guid("{3A94B34F-5924-471E-BAE8-D30AB49B1DE9}");
            private static readonly Guid dis43 = new Guid("{7C08DEDF-4C83-4ACE-BACB-87683D7FE9AA}");
            private static readonly Guid dis44 = new Guid("{DD00A50F-CB35-4080-AE7B-A5C26BE9C0A9}");
            private static readonly Guid dis45 = new Guid("{0DE01029-2BF3-4042-83DA-5633C397C2A2}");
            private static readonly Guid dis46 = new Guid("{940A7FA2-A4D6-401F-AF8A-8DD67DEA2295}");
            private static readonly Guid dis47 = new Guid("{2FF3A76E-F7C6-4F44-8F91-192B431A1106}");
            private static readonly Guid dis48 = new Guid("{C8F6A8BF-F51F-427A-AA53-EA7CD1B63C7C}");
            private static readonly Guid dis49 = new Guid("{04EB0012-E8E7-4DA6-9BA5-28ED0F8BBE69}");
            private static readonly Guid dis50 = new Guid("{0648E502-3EA2-4FD6-B505-78331D7F337D}");
            private static readonly Guid dis51 = new Guid("{61806A08-B216-497E-889F-C5FFB2DDF4F2}");
            private static readonly Guid dis52 = new Guid("{358379BD-F7CB-43BF-B0D5-3E4DF0EBC586}");
            private static readonly Guid dis53 = new Guid("{073217E4-BD96-4EA5-BC1B-777D2E248294}");
            private static readonly Guid dis54 = new Guid("{711DEA4C-C051-4B8E-AF89-143326DBDDCD}");
            private static readonly Guid dis55 = new Guid("{091A243B-2565-45D7-B02D-6FF2E2A02A1D}");
            private static readonly Guid dis56 = new Guid("{76BC0B28-12D5-4502-B072-3940D0949B7E}");
            private static readonly Guid dis57 = new Guid("{42C3CB7B-37CC-4D40-9BB3-2B30FBE07D0F}");
            private static readonly Guid dis58 = new Guid("{26202293-B601-4F4B-9142-FDDEFB8063E6}");
            private static readonly Guid dis59 = new Guid("{CDB05FCA-44EA-4713-99EC-9E1D80271FEE}");
            private static readonly Guid dis60 = new Guid("{E491CD42-EA35-417D-8C1B-59287BD2D8E8}");
            private static readonly Guid dis61 = new Guid("{F50FD5B9-16F3-45AC-BAEA-02640F02912B}");
            private static readonly Guid dis62 = new Guid("{FCB537D1-6726-40E2-B484-91CE9C698895}");
            private static readonly Guid dis63 = new Guid("{4E958C6E-E319-4211-B047-F56EB69EF4CC}");
            private static readonly Guid dis64 = new Guid("{CA7BCCC5-EEC2-4C1A-A1AB-39AC3913E296}");
            private static readonly Guid dis65 = new Guid("{FA27FF69-6B10-4A6C-8A5C-9E6447C5604B}");
            private static readonly Guid dis66 = new Guid("{2AB3580D-1B0E-4824-986D-A14D2AE6D281}");
            private static readonly Guid dis67 = new Guid("{8D9BC28D-3177-4663-8F26-1F6F5C7FCF42}");
            private static readonly Guid dis68 = new Guid("{D0865B0B-793D-438C-9D53-A71FF4D599F1}");
            private static readonly Guid dis69 = new Guid("{B1A29E3B-3F80-49DF-A94F-6F3F91165112}");
            private static readonly Guid dis70 = new Guid("{8DDDADBF-710E-4182-A912-C98604080BAF}");
            private static readonly Guid dis71 = new Guid("{4ED420A7-1561-417B-AC49-B0123A6A0E03}");
            private static readonly Guid dis72 = new Guid("{0E5917DA-8BFE-4E50-819D-065AF47B5CAD}");
            private static readonly Guid dis73 = new Guid("{609F1D57-023D-400F-B3FE-F8EE9D20BEB9}");
            private static readonly Guid dis74 = new Guid("{18988DC4-FFD1-4DA5-BB90-5738F3BA08BF}");
            private static readonly Guid dis75 = new Guid("{6C7FF2AC-5394-4F7F-A085-709A7D0DCB70}");
            private static readonly Guid dis76 = new Guid("{3642C666-F83E-43C2-9D6E-29C9E527E22A}");
            private static readonly Guid dis77 = new Guid("{80AD3EBC-43A3-43D3-85FB-3DE5C2DA5364}");
            private static readonly Guid dis78 = new Guid("{03C74B83-32EC-48AC-B4C8-E4EAF24D089B}");
            private static readonly Guid dis79 = new Guid("{3D17C5D9-315E-4D23-8352-3367C0FDAF88}");
            private static readonly Guid dis80 = new Guid("{09824519-A477-4901-AA1C-7C97BE816B9A}");
            private static readonly Guid dis81 = new Guid("{DF466856-AB99-4C6B-99B8-A0172FED7964}");

            private static readonly Guid metropolis = new Guid("{0504E82A-7468-4DAF-BCA1-F31163241C02}");
            private static readonly Guid women = new Guid("{BC064CB6-0EF7-4535-9208-4288EA6EFD21}");

            // Group disability
            private static readonly Guid group1 = new Guid("{8B2A4C4E-DBE7-4A49-AB2B-D788C22DC3D5}");
            private static readonly Guid group2 = new Guid("{4E62F6B9-BDFA-4D92-97A0-1E0B3AEDB473}");
            private static readonly Guid group3 = new Guid("{07F39539-FF02-4F5F-A4EA-D403AFB679CF}");
            private static readonly Guid groupChild = new Guid("{0643F3EA-94C5-4888-BCD3-2CCADE7A3AE4}");
            // By whom pointed
            private static readonly Guid msec = new Guid("{A593BA5C-714F-4ECE-8346-B88BCD225A9E}");
            private static readonly Guid rupoi = new Guid("{BBD3975D-9B3E-404F-A9BA-ED0C787E6817}");
            private static readonly Guid csm = new Guid("{CE62443C-C025-400E-86E2-639B5A973A5C}");
            private static readonly Guid ssu = new Guid("{9B989C2C-1ECF-4962-99C6-75EDCC7E883D}");
            /*--------------------------------------------------------------------------------------------------------------------*/
            private static Guid oneService = new Guid("{267ACE41-9834-4593-B6A2-DAD0D0A72293}"); //Цель направления в ЦРЛОВЗ-Реабилитация 

            private static Guid[] twoService = new Guid[]
            {
            new Guid("{129333EE-7948-485B-8406-B5D7E378DAA5}"), /*Протезирование*/
            new Guid("{007B16CE-181A-47E9-9897-BFD1D08090CF}"), /*Реабилитация и протезирование*/ 
            new Guid("{94FB1C58-D16E-46D8-A159-73D1106A343E}")  /*Ремонт ПОИ*/
            };

            public static List<ReportItem> Build(WorkflowContext context, int year, int month, Guid userId, UserInfo userInfo)
            {
                if (year < 2011 || year > 3000)
                    throw new ApplicationException("Ошибка в значении года!");
                if (month < 1 || month > 12)
                    throw new ApplicationException("Ошибка в значении месяца!");
                if (userInfo.OrganizationId == null)
                    throw new ApplicationException("Не могу создать заявку! Организация не указана!");

                var docRepo = context.Documents;
                var qb = new QueryBuilder(ReportDocId, userId);
                qb.Where("Year").Eq(year).And("Month").Eq(month).And("Organization").Eq(userInfo.OrganizationId)/*.And("&State").IsNull()*/;

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("&Id");

                var docs = new List<Guid>();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    if (reader.Read())
                        docs.Add(reader.Reader.GetGuid(0));
                }

                Doc report;
                var rowsAttrDefId = new Guid("{CEE41BE3-E0F6-410C-AD41-23FB3DDF90A3}");
                if (docs.Count > 0)
                {
                    report = docRepo.LoadById(docs[0]);
                    docRepo.ClearAttrDocList(report.Id, rowsAttrDefId);
                }
                else
                    report = docRepo.New(ReportDocId);

                var names = new Guid[]{
                                       field129, field128, field127,
                                       field126, field125, field124, field123, field122, field121, field120, field119, field118, field117, field116,
                                       field115, field114, field113, field112, field111, field110, field109, field108, field107, field106,
                                       field105, field104, field103, field102, field101, field100, field99, field98, field97, field96,
                                       field95, field94, field93, field92, field91, field90, field89, field88, field87, field86, field85,
                                       field84, field83, field82, field81, field80, field79, field78, field77, field76, field75, field74,
                                       field73, field72, field71, field70, field69, field68, field67, field66, field65, field64, field63,
                                       field62, field61, field60, field59, field58, field57, field56, field55, field54, field53, field52,
                                       field51, field50, field49, field48, field47, field46, field45, field44, field43, field42, field41,
                                       field40_1, field40, field39, field38, field37, field36, field35, field34, field33, field32_1,
                                       field32, field31, field30, field29, field28, field27, field26, field25, field24_3, field24_2, field24_1, field24, field23, field22,
                                       field21, field20, field19, field18, field17, field16, field15, field14, field13, field12, field11_1,
                                       field11, field10, field9, field8, field7, field6, field5, field4, field3, field2, field1
                                   };

                var items = new List<ReportItem>();
                foreach (var i1 in names)
                {
                    var item = new ReportItem(); //docRepo.New(reportItemDefId);
                    item.FieldName = i1;
                    item.FieldNameText = context.Enums.GetValue(i1).Value;
                    items.Add(item);
                }
                CalcItems(context, userId, (Guid)userInfo.OrganizationId, year, month,
                    items[134], items[133], items[132], items[131], items[130], items[129], items[128], items[127], items[126], items[125],
                    items[124], items[123], items[122], items[121], items[120], items[119], items[118], items[117], items[116], items[115],
                    items[114], items[113], items[112], items[111], items[110], items[109], items[108], items[107], items[106], items[105], items[104],
                    items[103], items[102], items[101], items[100], items[99], items[98], items[97], items[96], items[95], items[94], items[93], items[92],
                    items[91], items[90], items[89], items[88], items[87], items[86], items[85], items[84], items[83], items[82], items[81], items[80],
                    items[79], items[78], items[77], items[76], items[75], items[74], items[73], items[72], items[71], items[70], items[69], items[68],
                    items[67], items[66], items[65], items[64], items[63], items[62], items[61], items[60], items[59], items[58], items[57], items[56],
                    items[55], items[54], items[53], items[52], items[51], items[50], items[49], items[48], items[47], items[46], items[45], items[44],
                    items[43], items[42], items[41], items[40], items[39], items[38], items[37], items[36], items[35], items[34], items[33], items[32],
                    items[31], items[30], items[29], items[28], items[27], items[26], items[25], items[24], items[23], items[22], items[21], items[20],
                    items[19], items[18], items[17], items[16], items[15], items[14], items[13], items[12], items[11], items[10], items[9], items[8],
                    items[7], items[6], items[5], items[4], items[3], items[2], items[1], items[0]);

                GetAllEvents(context, report, (Guid)userInfo.OrganizationId, year, month);

                return items;
            }
            private static void GetAllEvents(WorkflowContext context, Doc report, Guid orgId, int year, int month)
            {
                var fd = new DateTime(year, month, 1);
                var ld = fd.AddMonths(1).AddDays(-1);
                var qb = new QueryBuilder(WeekReportDefId);
                qb.Where("&OrgId").Eq(orgId).And("From").Ge(fd).And("To").Le(ld);
                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var events = new List<object>();
                query.AddAttribute("Event");
                query.AddOrderAttribute("Event");
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                        if (!reader.IsDbNull(0))
                            events.Add(reader.GetValue(0));
                }
                string even = "";
                foreach (var e in events)
                    even += e + "\n";
                var eLength = even.Length;
                report["Event"] = even.Substring(0, eLength < 800 ? eLength - 0 : 799);
            }
            public static void CalcItems(WorkflowContext context, Guid userId, Guid orgId, int year, int month,
                ReportItem item1, ReportItem item2, ReportItem item3, ReportItem item4, ReportItem item5, ReportItem item6, ReportItem item7, ReportItem item8, ReportItem item9, ReportItem item10,
                ReportItem item11, ReportItem item11_1, ReportItem item12, ReportItem item13, ReportItem item14, ReportItem item15, ReportItem item16, ReportItem item17, ReportItem item18, ReportItem item19,
                ReportItem item20, ReportItem item21, ReportItem item22, ReportItem item23, ReportItem item24, ReportItem item24_1, ReportItem item24_2, ReportItem item24_3, ReportItem item25, ReportItem item26, ReportItem item27, ReportItem item28,
                ReportItem item29, ReportItem item30, ReportItem item31, ReportItem item32, ReportItem item32_1, ReportItem item33, ReportItem item34, ReportItem item35, ReportItem item36, ReportItem item37, ReportItem item38, ReportItem item39, ReportItem item40, ReportItem item40_1, ReportItem item41, ReportItem item42, ReportItem item43,
                ReportItem item44, ReportItem item45, ReportItem item46, ReportItem item47, ReportItem item48, ReportItem item49, ReportItem item50, ReportItem item51, ReportItem item52, ReportItem item53, ReportItem item54, ReportItem item55, ReportItem item56, ReportItem item57, ReportItem item58,
                ReportItem item59, ReportItem item60, ReportItem item61, ReportItem item62, ReportItem item63, ReportItem item64, ReportItem item65, ReportItem item66, ReportItem item67, ReportItem item68, ReportItem item69, ReportItem item70, ReportItem item71, ReportItem item72, ReportItem item73,
                ReportItem item74, ReportItem item75, ReportItem item76, ReportItem item77, ReportItem item78, ReportItem item79, ReportItem item80, ReportItem item81, ReportItem item82, ReportItem item83, ReportItem item84, ReportItem item85, ReportItem item86, ReportItem item87, ReportItem item88,
                ReportItem item89, ReportItem item90, ReportItem item91, ReportItem item92, ReportItem item93, ReportItem item94, ReportItem item95, ReportItem item96, ReportItem item97, ReportItem item98, ReportItem item99, ReportItem item100, ReportItem item101, ReportItem item102, ReportItem item103,
                ReportItem item104, ReportItem item105, ReportItem item106, ReportItem item107, ReportItem item108, ReportItem item109, ReportItem item110, ReportItem item111, ReportItem item112, ReportItem item113, ReportItem item114, ReportItem item115, ReportItem item116, ReportItem item117, ReportItem item118,
                ReportItem item119, ReportItem item120, ReportItem item121, ReportItem item122, ReportItem item123, ReportItem item124, ReportItem item125, ReportItem item126, ReportItem item127, ReportItem item128, ReportItem item129)
            {
                var fd = new DateTime(year, month, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));

                var qb = new QueryBuilder(cardDefId, userId);
                qb.Where("&OrgId").Eq(orgId).And("DateOfReceipt").Le(ld).And("OrderDateDef").Ge(fd).And("&State").Neq(new Guid("ca1157a0-fddf-4c90-9692-7cdb47ccc7c2")); //Отказанные
                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var applicant = query.JoinSource(query.Source, ApplicantDefId, SqlSourceJoinType.Inner, "Applicant");
                var vypiska = query.JoinSource(query.Source, vypiskaDefId, SqlSourceJoinType.LeftOuter, "CardOfThePatient");
                query.AddAttributes(new[] {
                                          "&Id", "GroupDisability", "DirectExamination", "HaveAttendant", "FirstType"
                                      });
                query.AddAttribute(applicant, "Sex");
                query.AddAttribute(query.Source, "OrderDateDef");
                query.AddAttribute(query.Source, "DateOfReceipt");
                query.AddAttribute(vypiska, "ResultRehabilitation");
                query.AddAttribute(vypiska, "DateExtraction");
                var list = new List<Guid>();
                var table = new DataTable();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }
                int nextMonth = 0;
                int str13 = 0;
                int str14 = 0;
                int str3 = 0;
                foreach (DataRow row in table.Rows)
                {
                    var cardObj = row[0];
                    if (!(cardObj is DBNull))
                    {
                        var cardId = (Guid)cardObj;
                        if (!list.Contains(cardId))
                        {
                            list.Add(cardId);

                            var group = row[1] is DBNull ? Guid.Empty : (Guid)row[1];
                            var examin = row[2] is DBNull ? Guid.Empty : (Guid)row[2];
                            bool attendant = row[3] is DBNull ? false : (bool)row[3];
                            bool prim = row[4] is DBNull ? false : (bool)row[4];
                            var sex = row[5] is DBNull ? Guid.Empty : (Guid)row[5];
                            var orderDateDef = row[6] is DBNull ? DateTime.MaxValue : (DateTime)row[6];
                            var reciptDateDef = row[7] is DBNull ? DateTime.MaxValue : (DateTime)row[7];
                            var resultReab = row[8] is DBNull ? Guid.Empty : (Guid)row[8];
                            var vypiskaDate = row[9] is DBNull ? DateTime.MaxValue : (DateTime)row[9];
                            if (orderDateDef >= fd && reciptDateDef < fd)
                            {
                                nextMonth++;
                                if (nextMonth != null && attendant)
                                {
                                    //строка №2
                                    item126.StringCount = item126.StringCount != string.Empty ? (Convert.ToInt32(item126.StringCount) + 1).ToString() : "1";
                                }
                            }
                            //строка №13
                            if (prim && examin == new Guid("{007B16CE-181A-47E9-9897-BFD1D08090CF}"))
                            {
                                str13++;
                            }
                            //строка №14
                            if (prim && examin == oneService)
                            {
                                str14++;
                            }
                            //строка № 15                           
                            if (group == groupChild)
                            {
                                item12.StringCount = item12.StringCount != string.Empty ? (Convert.ToInt32(item12.StringCount) + 1).ToString() : "1";
                                if (sex == women)
                                    item12.Women = (int?)item12.Women + 1 ?? 1;
                                else if (sex != Guid.Empty)
                                    item12.Men = (int?)item12.Men + 1 ?? 1;
                            }
                            //строка № 16                        
                            if (group == group1)
                            {
                                item13.StringCount = item13.StringCount != string.Empty ? (Convert.ToInt32(item13.StringCount) + 1).ToString() : "1";
                                if (sex == women)
                                    item13.Women = (int?)item13.Women + 1 ?? 1;
                                else if (sex != Guid.Empty)
                                    item13.Men = (int?)item13.Men + 1 ?? 1;
                            }
                            //строка № 17                        
                            if (group == group2)
                            {
                                item14.StringCount = item14.StringCount != string.Empty ? (Convert.ToInt32(item14.StringCount) + 1).ToString() : "1";
                                if (sex == women)
                                    item14.Women = (int?)item14.Women + 1 ?? 1;
                                else if (sex != Guid.Empty)
                                    item14.Men = (int?)item14.Men + 1 ?? 1;
                            }
                            //строка № 18                        
                            if (group == group3)
                            {
                                item15.StringCount = item15.StringCount != string.Empty ? (Convert.ToInt32(item15.StringCount) + 1).ToString() : "1";
                                if (sex == women)
                                    item15.Women = (int?)item15.Women + 1 ?? 1;
                                else if (sex != Guid.Empty)
                                    item15.Men = (int?)item15.Men + 1 ?? 1;
                            }
                            if (attendant)
                            {
                                item5.StringCount = item5.StringCount != string.Empty ? (Convert.ToInt32(item5.StringCount) + 1).ToString() : "1";
                                item6.StringCount = item6.StringCount != string.Empty ? (Convert.ToInt32(item6.StringCount) + CountDays(context.GetDynaDoc(cardId), month, year, context)).ToString() : CountDays(context.GetDynaDoc(cardId), month, year, context).ToString();
                            }
                            item4.StringCount = item4.StringCount != string.Empty ? (Convert.ToInt32(item4.StringCount) + CountDays(context.GetDynaDoc(cardId), month, year, context)).ToString() : CountDays(context.GetDynaDoc(cardId), month, year, context).ToString();

                            if (twoService.Contains(examin))
                            {
                                item7.StringCount = item7.StringCount != string.Empty ? (Convert.ToInt32(item7.StringCount) + 1).ToString() : "1";
                                item8.StringCount = item8.StringCount != string.Empty ? (Convert.ToInt32(item8.StringCount) + CountDays(context.GetDynaDoc(cardId), month, year, context)).ToString() : CountDays(context.GetDynaDoc(cardId), month, year, context).ToString();
                            }
                            else if (examin == oneService)
                            {
                                item9.StringCount = item9.StringCount != string.Empty ? (Convert.ToInt32(item9.StringCount) + 1).ToString() : "1";
                                item10.StringCount = item10.StringCount != string.Empty ? (Convert.ToInt32(item10.StringCount) + CountDays(context.GetDynaDoc(cardId), month, year, context)).ToString() : CountDays(context.GetDynaDoc(cardId), month, year, context).ToString();
                            }
                            if (vypiskaDate <= ld && vypiskaDate >= fd)
                            {
                                //строка №44                        
                                if ((Guid)resultReab == new Guid("{2A02FEBE-51B0-413C-9F44-73DE432282A8}"))
                                {
                                    item127.StringCount = item127.StringCount != string.Empty ? (Convert.ToInt32(item127.StringCount) + 1).ToString() : "1";
                                }
                                //строка №45
                                if ((Guid)resultReab == new Guid("{E459834F-0254-453D-A871-9197BCCD8B44}"))
                                {
                                    item128.StringCount = item128.StringCount != string.Empty ? (Convert.ToInt32(item128.StringCount) + 1).ToString() : "1";
                                }
                                //строка №46
                                if ((Guid)resultReab == new Guid("{0941FAAF-AAA8-44A8-969D-FCA9A68C6BD5}"))
                                {
                                    item129.StringCount = item129.StringCount != string.Empty ? (Convert.ToInt32(item129.StringCount) + 1).ToString() : "1";
                                }
                            }
                        }
                    }
                }
                item125.StringCount = nextMonth.ToString(); //строка №1
                item3.StringCount = list.Count.ToString(); //количество стат. кард на тек. месяц                 
                item11.StringCount = str13.ToString();
                item11_1.StringCount = str14.ToString();

                if (string.IsNullOrEmpty((string)item7.StringCount)) item7.StringCount = "0";
                if (string.IsNullOrEmpty((string)item8.StringCount)) item8.StringCount = "0";
                if (string.IsNullOrEmpty((string)item9.StringCount)) item9.StringCount = "0";
                if (string.IsNullOrEmpty((string)item10.StringCount)) item10.StringCount = "0";
                item40.StringCount = "0";
                item41.StringCount = "0";
                item42.StringCount = "0";
                item43.StringCount = "0";

                item2.StringCount = (Convert.ToInt32(item6.StringCount) + Convert.ToInt32(item4.StringCount)).ToString();
                item26.StringCount = item7.StringCount;
                //строка №5 
                item16.StringCount = item3.StringCount;

                GetProcedures(context, year, month, userId, orgId, new List<ReportItem> { item17, item18, item19, item22, item23, item24, item25, item20,
                                                                               item21, item27, item28, item29, item24_1, item24_2, item24_3
                                                                             });
                GetNextItems(context, item30, item31, item32, item32_1, item33, item34, item35, item36, item37, item38, item39, item44, item45, item46, item47, item48,
                            item49, item50, item51, item52, item53, item54, item55, item56, item57, item58, item59, item60, item61, item62, item63, item64,
                            item65, item66, item67, item68, item69, item70, item71, item72, item73, item74, item75, item76, item77, item78, item79,
                            item80, item81, item82, item83, item84, item85, item86, item87, item88, item89, item90, item91, item92, item93, item94,
                            item95, item96, item97, item98, item99, item100, item101, item102, item103, item104, item105, item106, item107, item108,
                            item109, item110, item111, item112, item113, item114, item115, item116, item117, item118, item119, item120, item121,
                            item122, item123, item124, item40, item40_1, item41, item42, item43, fd, ld);

                //строка №3 = строка №5 + строка №2 + строка №21
                item1.StringCount = (Convert.ToInt32(item3.StringCount) + Convert.ToInt32(item126.StringCount) + Convert.ToInt32(item42.StringCount)).ToString();
            }

            private static void GetNextItems(WorkflowContext context, ReportItem item30, ReportItem item31, ReportItem item32, ReportItem item32_1, ReportItem item33, ReportItem item34, ReportItem item35, ReportItem item36,
                                ReportItem item37, ReportItem item38, ReportItem item39, ReportItem item44, ReportItem item45, ReportItem item46, ReportItem item47, ReportItem item48, ReportItem item49, ReportItem item50, ReportItem item51, ReportItem item52,
                                ReportItem item53, ReportItem item54, ReportItem item55, ReportItem item56, ReportItem item57, ReportItem item58, ReportItem item59, ReportItem item60, ReportItem item61, ReportItem item62, ReportItem item63, ReportItem item64,
                                ReportItem item65, ReportItem item66, ReportItem item67, ReportItem item68, ReportItem item69, ReportItem item70, ReportItem item71, ReportItem item72, ReportItem item73, ReportItem item74, ReportItem item75, ReportItem item76,
                                ReportItem item77, ReportItem item78, ReportItem item79, ReportItem item80, ReportItem item81, ReportItem item82, ReportItem item83, ReportItem item84, ReportItem item85, ReportItem item86, ReportItem item87, ReportItem item88,
                                ReportItem item89, ReportItem item90, ReportItem item91, ReportItem item92, ReportItem item93, ReportItem item94, ReportItem item95, ReportItem item96, ReportItem item97, ReportItem item98, ReportItem item99, ReportItem item100,
                                ReportItem item101, ReportItem item102, ReportItem item103, ReportItem item104, ReportItem item105, ReportItem item106, ReportItem item107, ReportItem item108, ReportItem item109, ReportItem item110, ReportItem item111,
                                ReportItem item112, ReportItem item113, ReportItem item114, ReportItem item115, ReportItem item116, ReportItem item117, ReportItem item118, ReportItem item119, ReportItem item120, ReportItem item121, ReportItem item122,
                                ReportItem item123, ReportItem item124, ReportItem item40, ReportItem item40_1, ReportItem item41, ReportItem item42, ReportItem item43, DateTime fd, DateTime ld)
            {
                var orderDefId = new Guid("{DA9CBB90-6527-41DA-A25E-690EFE24AE8A}");
                var qb = new QueryBuilder(orderDefId, context.UserId);
                qb.Where("DateExtraction").Ge(fd).And("DateExtraction").Le(ld).And("&State").Eq(new Guid("79417A2A-365D-473B-A09B-3230CAA17D9D")); //выписанные
                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var cardSrc = query.JoinSource(query.Source, cardDefId, SqlSourceJoinType.Inner, "CardOfThePatient");
                query.AddAttribute(query.Source, "&Id");
                query.AddAttribute(cardSrc, "HaveAttendant");
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    int i40 = 0;
                    int i40_1 = 0;
                    while (reader.Read())
                    {
                        i40++;
                        var haveAttendant = reader.IsDbNull(1) ? false : reader.GetBoolean(1);
                        if (haveAttendant)
                            i40_1++;
                    }
                    //строка 42, 43             
                    item40.StringCount = i40.ToString();
                    item40_1.StringCount = i40_1.ToString();
                }

                qb = new QueryBuilder(cardDefId, context.UserId);
                qb.Where("DateOfReceipt").Ge(fd).And("DateOfReceipt").Le(ld).And("&State").Neq(new Guid("ca1157a0-fddf-4c90-9692-7cdb47ccc7c2"));
                query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("&Id", SqlQuerySummaryFunction.Count);
                query.AddAttribute("HaveAttendant");
                query.AddAttribute("WhoDirected");
                query.AddAttribute("Obl");
                query.AddAttribute("Rag");
                query.AddGroupAttribute("HaveAttendant");
                query.AddGroupAttribute("WhoDirected");
                query.AddGroupAttribute("Obl");
                query.AddGroupAttribute("Rag");
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        int c = reader.GetInt32(0);
                        bool attend = reader.Reader.IsDBNull(1) ? false : reader.GetBoolean(1);
                        var directId = !reader.Reader.IsDBNull(2) ? reader.GetGuid(2) : Guid.Empty;
                        var region = !reader.Reader.IsDBNull(3) ? reader.GetGuid(3) : Guid.Empty;
                        var distr = !reader.Reader.IsDBNull(4) ? reader.GetGuid(4) : Guid.Empty;

                        //строка №20
                        item41.StringCount = (Convert.ToInt32(item41.StringCount) + c).ToString();

                        //строка №21
                        if (attend)
                        {
                            item42.StringCount = (Convert.ToInt32(item42.StringCount) + c).ToString();
                        }
                        item43.StringCount = (Convert.ToInt32(item43.StringCount) + c).ToString();
                        if (directId == rupoi)
                        {
                            item30.StringCount = (Convert.ToInt32(item30.StringCount) + c).ToString();
                        }
                        else if (directId == msec)
                            item31.StringCount = (Convert.ToInt32(item31.StringCount) + c).ToString();
                        else if (directId == csm)
                            item32.StringCount = (Convert.ToInt32(item32.StringCount) + c).ToString();
                        else if (directId == ssu)
                            item32_1.StringCount = (Convert.ToInt32(item32_1.StringCount) + c).ToString();
                        if (region == region1 || region == metropolis)
                            item33.StringCount = item33.StringCount != string.Empty ? (Convert.ToInt32(item33.StringCount) + c).ToString() : c.ToString();
                        else if (region == region2)
                            item34.StringCount = item34.StringCount != string.Empty ? (Convert.ToInt32(item34.StringCount) + c).ToString() : c.ToString();
                        else if (region == region3)
                            item35.StringCount = item35.StringCount != string.Empty ? (Convert.ToInt32(item35.StringCount) + c).ToString() : c.ToString();
                        else if (region == region4)
                            item36.StringCount = item36.StringCount != string.Empty ? (Convert.ToInt32(item36.StringCount) + c).ToString() : c.ToString();
                        else if (region == region5)
                            item37.StringCount = item37.StringCount != string.Empty ? (Convert.ToInt32(item37.StringCount) + c).ToString() : c.ToString();
                        else if (region == region6)
                            item38.StringCount = item38.StringCount != string.Empty ? (Convert.ToInt32(item38.StringCount) + c).ToString() : c.ToString();
                        else if (region == region7)
                            item39.StringCount = item39.StringCount != string.Empty ? (Convert.ToInt32(item39.StringCount) + c).ToString() : c.ToString();

                        if (distr == dis1)
                            item44.StringCount = item44.StringCount != string.Empty ? (Convert.ToInt32(item44.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis2)
                            item45.StringCount = item45.StringCount != string.Empty ? (Convert.ToInt32(item45.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis3)
                            item46.StringCount = item46.StringCount != string.Empty ? (Convert.ToInt32(item46.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis4)
                            item47.StringCount = item47.StringCount != string.Empty ? (Convert.ToInt32(item47.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis5)
                            item48.StringCount = item48.StringCount != string.Empty ? (Convert.ToInt32(item48.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis6)
                            item49.StringCount = item49.StringCount != string.Empty ? (Convert.ToInt32(item49.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis7)
                            item50.StringCount = item50.StringCount != string.Empty ? (Convert.ToInt32(item50.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis8)
                            item51.StringCount = item51.StringCount != string.Empty ? (Convert.ToInt32(item51.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis9)
                            item52.StringCount = item52.StringCount != string.Empty ? (Convert.ToInt32(item52.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis10)
                            item53.StringCount = item53.StringCount != string.Empty ? (Convert.ToInt32(item53.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis11)
                            item54.StringCount = item54.StringCount != string.Empty ? (Convert.ToInt32(item54.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis12)
                            item55.StringCount = item55.StringCount != string.Empty ? (Convert.ToInt32(item55.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis13)
                            item56.StringCount = item56.StringCount != string.Empty ? (Convert.ToInt32(item56.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis14)
                            item57.StringCount = item57.StringCount != string.Empty ? (Convert.ToInt32(item57.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis15)
                            item58.StringCount = item58.StringCount != string.Empty ? (Convert.ToInt32(item58.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis16)
                            item59.StringCount = item59.StringCount != string.Empty ? (Convert.ToInt32(item59.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis17)
                            item60.StringCount = item60.StringCount != string.Empty ? (Convert.ToInt32(item60.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis18)
                            item61.StringCount = item61.StringCount != string.Empty ? (Convert.ToInt32(item61.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis19)
                            item62.StringCount = item62.StringCount != string.Empty ? (Convert.ToInt32(item62.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis20)
                            item63.StringCount = item63.StringCount != string.Empty ? (Convert.ToInt32(item63.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis21)
                            item64.StringCount = item64.StringCount != string.Empty ? (Convert.ToInt32(item64.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis22)
                            item65.StringCount = item65.StringCount != string.Empty ? (Convert.ToInt32(item65.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis23)
                            item66.StringCount = item66.StringCount != string.Empty ? (Convert.ToInt32(item66.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis24)
                            item67.StringCount = item67.StringCount != string.Empty ? (Convert.ToInt32(item67.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis25)
                            item68.StringCount = item68.StringCount != string.Empty ? (Convert.ToInt32(item68.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis26)
                            item69.StringCount = item69.StringCount != string.Empty ? (Convert.ToInt32(item69.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis27)
                            item70.StringCount = item70.StringCount != string.Empty ? (Convert.ToInt32(item70.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis28)
                            item71.StringCount = item71.StringCount != string.Empty ? (Convert.ToInt32(item71.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis29)
                            item72.StringCount = item72.StringCount != string.Empty ? (Convert.ToInt32(item72.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis30)
                            item73.StringCount = item73.StringCount != string.Empty ? (Convert.ToInt32(item73.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis31)
                            item74.StringCount = item74.StringCount != string.Empty ? (Convert.ToInt32(item74.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis32)
                            item75.StringCount = item75.StringCount != string.Empty ? (Convert.ToInt32(item75.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis33)
                            item76.StringCount = item76.StringCount != string.Empty ? (Convert.ToInt32(item76.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis34)
                            item77.StringCount = item77.StringCount != string.Empty ? (Convert.ToInt32(item77.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis35)
                            item78.StringCount = item78.StringCount != string.Empty ? (Convert.ToInt32(item78.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis36)
                            item79.StringCount = item79.StringCount != string.Empty ? (Convert.ToInt32(item79.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis37)
                            item80.StringCount = item80.StringCount != string.Empty ? (Convert.ToInt32(item80.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis38)
                            item81.StringCount = item81.StringCount != string.Empty ? (Convert.ToInt32(item81.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis39)
                            item82.StringCount = item82.StringCount != string.Empty ? (Convert.ToInt32(item82.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis40)
                            item83.StringCount = item83.StringCount != string.Empty ? (Convert.ToInt32(item83.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis41)
                            item84.StringCount = item84.StringCount != string.Empty ? (Convert.ToInt32(item84.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis42)
                            item85.StringCount = item85.StringCount != string.Empty ? (Convert.ToInt32(item85.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis43)
                            item86.StringCount = item86.StringCount != string.Empty ? (Convert.ToInt32(item86.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis44)
                            item87.StringCount = item87.StringCount != string.Empty ? (Convert.ToInt32(item87.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis45)
                            item88.StringCount = item88.StringCount != string.Empty ? (Convert.ToInt32(item88.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis46)
                            item89.StringCount = item89.StringCount != string.Empty ? (Convert.ToInt32(item89.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis47)
                            item90.StringCount = item90.StringCount != string.Empty ? (Convert.ToInt32(item90.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis48)
                            item91.StringCount = item91.StringCount != string.Empty ? (Convert.ToInt32(item91.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis49)
                            item92.StringCount = item92.StringCount != string.Empty ? (Convert.ToInt32(item92.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis50)
                            item93.StringCount = item93.StringCount != string.Empty ? (Convert.ToInt32(item93.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis51)
                            item94.StringCount = item94.StringCount != string.Empty ? (Convert.ToInt32(item94.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis52)
                            item95.StringCount = item95.StringCount != string.Empty ? (Convert.ToInt32(item95.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis53)
                            item96.StringCount = item96.StringCount != string.Empty ? (Convert.ToInt32(item96.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis54)
                            item97.StringCount = item97.StringCount != string.Empty ? (Convert.ToInt32(item97.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis55)
                            item98.StringCount = item98.StringCount != string.Empty ? (Convert.ToInt32(item98.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis56)
                            item99.StringCount = item99.StringCount != string.Empty ? (Convert.ToInt32(item99.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis57)
                            item100.StringCount = item100.StringCount != string.Empty ? (Convert.ToInt32(item100.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis58)
                            item101.StringCount = item101.StringCount != string.Empty ? (Convert.ToInt32(item101.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis59)
                            item102.StringCount = item102.StringCount != string.Empty ? (Convert.ToInt32(item102.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis60)
                            item103.StringCount = item103.StringCount != string.Empty ? (Convert.ToInt32(item103.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis61)
                            item104.StringCount = item104.StringCount != string.Empty ? (Convert.ToInt32(item104.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis62)
                            item105.StringCount = item105.StringCount != string.Empty ? (Convert.ToInt32(item105.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis63)
                            item106.StringCount = item106.StringCount != string.Empty ? (Convert.ToInt32(item106.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis64)
                            item107.StringCount = item107.StringCount != string.Empty ? (Convert.ToInt32(item107.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis65)
                            item108.StringCount = item108.StringCount != string.Empty ? (Convert.ToInt32(item108.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis66)
                            item109.StringCount = item109.StringCount != string.Empty ? (Convert.ToInt32(item109.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis67)
                            item110.StringCount = item110.StringCount != string.Empty ? (Convert.ToInt32(item110.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis68)
                            item111.StringCount = item111.StringCount != string.Empty ? (Convert.ToInt32(item111.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis69)
                            item112.StringCount = item112.StringCount != string.Empty ? (Convert.ToInt32(item112.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis70)
                            item113.StringCount = item113.StringCount != string.Empty ? (Convert.ToInt32(item113.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis71)
                            item114.StringCount = item114.StringCount != string.Empty ? (Convert.ToInt32(item114.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis72)
                            item115.StringCount = item115.StringCount != string.Empty ? (Convert.ToInt32(item115.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis73)
                            item116.StringCount = item116.StringCount != string.Empty ? (Convert.ToInt32(item116.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis74)
                            item117.StringCount = item117.StringCount != string.Empty ? (Convert.ToInt32(item117.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis75)
                            item118.StringCount = item118.StringCount != string.Empty ? (Convert.ToInt32(item118.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis76)
                            item119.StringCount = item119.StringCount != string.Empty ? (Convert.ToInt32(item119.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis77)
                            item120.StringCount = item120.StringCount != string.Empty ? (Convert.ToInt32(item120.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis78)
                            item121.StringCount = item121.StringCount != string.Empty ? (Convert.ToInt32(item121.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis79)
                            item122.StringCount = item122.StringCount != string.Empty ? (Convert.ToInt32(item122.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis80)
                            item123.StringCount = item123.StringCount != string.Empty ? (Convert.ToInt32(item123.StringCount) + c).ToString() : c.ToString();
                        else if (distr == dis81)
                            item124.StringCount = item124.StringCount != string.Empty ? (Convert.ToInt32(item124.StringCount) + c).ToString() : c.ToString();
                    }
                }
            }

            private static Dictionary<string, int> CardsDays = new Dictionary<string, int>();
            public static int CountDays(dynamic card, int month, int year, WorkflowContext context)
            {
                var days = 0;
                var fullMonth = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var beginMonth = new DateTime(year, month, 1);
                if (card.DateOfReceipt == null)
                    throw new ApplicationException("У карты под базовым номером " + card.RegNo.ToString() + " не указана дата поступления!");
                DateTime recDate = card.DateOfReceipt;
                DateTime outDate = card.OrderDateDef;
                if (outDate < beginMonth)
                    throw new ApplicationException("У карты под базовым номером " + card.RegNo.ToString() + " дата выписки меньше чем дата получения процедуры!");
                if (recDate < beginMonth)//дата поступления раньше начала месяца
                {
                    if (outDate > fullMonth)//дата выписки позже конца месяца
                        days = fullMonth.Day;//целый месяц
                    else//дата выписки не позже конца месяца
                        days = outDate.Day - 1; //день выписки не включительно
                }
                else //дата поступления не раньше или равно начала месяца
                {
                    if (outDate > fullMonth)//дата выписки позже конца месяца
                        days = (int)(fullMonth - recDate).TotalDays + 1;//разница конца месяца и даты поступления
                    else//дата выписки не позже конца месяца 
                        days = (int)(outDate - recDate).TotalDays;//разница даты выписки и даты поступления
                }
                var cardNo = card.RegNo.ToString();
                if (!CardsDays.ContainsKey(cardNo))
                    CardsDays.Add(cardNo, days);
                return days;
            }
            private static void GetProcedures(WorkflowContext context, int year, int month, Guid userId, Guid orgId, List<ReportItem> items)
            {
                var fd = new DateTime(year, month, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var procedureType = "процедур";
                var iprTypes = new Guid[]
                {
                new Guid("{B318D20E-11BC-4914-9672-C0D8930AC80A}"), //  Физиолечение
                new Guid("{558F5E14-B96F-4412-AA08-62F3310E43D3}"), //  Парафинотерапия
                new Guid("{0CB745A4-3EAE-40FD-9D6F-27823282382A}"), //  ЛФК              
                new Guid("{1B1334AB-09F6-460F-93E0-198F14388D00}"), //  Перевязки
                new Guid("{74437DF8-99B3-4E82-850F-B7340D93AEA6}"), //  Медикаментозная реабилитация
                new Guid("{F469BE29-7BD5-48DD-8496-4F4FE6D37DB4}"), //  Психологическая реабилитация
                new Guid("{9B14B46B-3882-413F-9F8C-07F4B769B4CD}")  //  Массаж
                };
                var iprTypes2 = new Guid[]
                {
                new Guid("{6794BD63-2DA0-45FE-94EA-7F58B91AC801}"), // Лечение зубов
                new Guid("{01AAAE7C-5559-449E-941A-66F1B3C59060}"), // Зубопротезирование
                new Guid("{166648BF-0F52-470E-ABC2-C9349E296315}"), // Трудотерапия(обучение основам компьютерной грамоте)
                new Guid("{96AAE67E-21CC-4C35-B180-21795DB64DE8}"), // Трудотерапия(обучение мелким ремонтом протезов и ортезов)
                new Guid("{3F752DA5-F243-4434-AF07-702EBB2C5D22}"),  // Трудотерапия(кройка и шитье)
                new Guid("{90526149-BDC0-46B7-BA0B-D0435EB16B92}"),  // ЭКГ 
                new Guid("{900A1115-C6EC-48D2-BA6C-5461C9935DE4}"),  // УЗДГ         
                new Guid("{9C4881D6-05CE-47EF-B027-E04A22D96CEE}")  // УЗИ
                };
                for (int i = 0; i < 7; i++)
                {
                    var iprId = iprTypes[i];
                    if (iprId == new Guid("{1B1334AB-09F6-460F-93E0-198F14388D00}")) /*Перевязки*/
                        procedureType = "перевязок";
                    if (iprId == new Guid("{0CB745A4-3EAE-40FD-9D6F-27823282382A}")) /*ЛФК*/
                        procedureType = "занятий";
                    if (iprId == new Guid("{F469BE29-7BD5-48DD-8496-4F4FE6D37DB4}")) /*Психологическая реабилитация */
                        procedureType = "бесед";
                    /* if (new Guid[] {                             
                                        new Guid("{90526149-BDC0-46B7-BA0B-D0435EB16B92}"), // ЭКГ
                                        new Guid("{900A1115-C6EC-48D2-BA6C-5461C9935DE4}"), // УЗДГ
                                        new Guid("{9C4881D6-05CE-47EF-B027-E04A22D96CEE}")   //УЗИ 
                                    }.Contains(iprId))
                                        procedureType = " чел.";    */
                    var item = items[i];
                    var query = new SqlQuery(context.DataContext, procedurDefId, userId);
                    query.JoinSource(query.Source, iprDefId, SqlSourceJoinType.Inner, "Rehabilitation");
                    query.AddCondition(ExpressionOperation.And, procedurDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, procedurDefId, "Date", ConditionOperation.GreatEqual, fd);
                    query.AddCondition(ExpressionOperation.And, procedurDefId, "Date", ConditionOperation.LessEqual, ld);
                    query.AddCondition(ExpressionOperation.And, iprDefId, "IndividualRehabilitationPlan", ConditionOperation.Equal, iprId);
                    query.AddAttribute("ProceduraCount", SqlQuerySummaryFunction.Sum);
                    using (var reader = new SqlQueryReader(context.DataContext, query))
                    {
                        if (reader.Read())
                        {
                            var count = !reader.Reader.IsDBNull(0) ? Convert.ToInt32(reader.GetValue(0)) : 0;
                            item.StringCount = string.Format("{1} чел. {0} {2}", count, CountPatient(iprId, orgId, fd, ld, context), procedureType);
                        }
                    }
                    procedureType = "процедур";
                }
                for (int i = 0; i < 8; i++)
                {
                    var iprId = iprTypes2[i];
                    if (new Guid[] {
                    new Guid("{166648BF-0F52-470E-ABC2-C9349E296315}"), // Трудотерапия(обучение основам компьютерной грамоте)
                    new Guid("{96AAE67E-21CC-4C35-B180-21795DB64DE8}"), // Трудотерапия(обучение мелким ремонтом протезов и ортезов)
                    new Guid("{3F752DA5-F243-4434-AF07-702EBB2C5D22}")}
                        .Contains(iprId))
                        procedureType = "занятий";
                    var item = items[i + 7];
                    var query = new SqlQuery(context.DataContext, procedurDefId, userId);
                    query.JoinSource(query.Source, iprDefId, SqlSourceJoinType.Inner, "Rehabilitation");
                    query.AddCondition(ExpressionOperation.And, procedurDefId, "&OrgId", ConditionOperation.Equal, orgId);
                    query.AddCondition(ExpressionOperation.And, procedurDefId, "Date", ConditionOperation.GreatEqual, fd);
                    query.AddCondition(ExpressionOperation.And, procedurDefId, "Date", ConditionOperation.LessEqual, ld);
                    query.AddCondition(ExpressionOperation.And, iprDefId, "IndividualRehabilitationPlan", ConditionOperation.Equal, iprId);
                    query.AddAttribute("ProceduraCount", SqlQuerySummaryFunction.Sum);
                    using (var reader = new SqlQueryReader(context.DataContext, query))
                    {
                        if (reader.Read())
                        {
                            var count = !reader.Reader.IsDBNull(0) ? Convert.ToInt32(reader.GetValue(0)) : 0;
                            item.StringCount = string.Format("{1} чел. {0} {2}", count, CountPatient(iprId, orgId, fd, ld, context), procedureType);
                        }
                    }
                    procedureType = "процедур";
                }
            }
            public static int CountPatient(Guid iprId, Guid orgId, DateTime fd, DateTime ld, WorkflowContext context)
            {
                var query = new SqlQuery(context.DataContext, cardDefId, context.UserId);
                var iprSrc = query.JoinSource(query.Source, iprDefId, SqlSourceJoinType.Inner, "IPRat");
                var appSrc = query.JoinSource(query.Source, ApplicantDefId, SqlSourceJoinType.Inner, "Applicant");
                query.JoinSource(iprSrc, procedurDefId, SqlSourceJoinType.Inner, "Rehabilitation");
                query.AddCondition(ExpressionOperation.And, procedurDefId, "&OrgId", ConditionOperation.Equal, orgId);
                query.AddCondition(ExpressionOperation.And, procedurDefId, "Date", ConditionOperation.GreatEqual, fd);
                query.AddCondition(ExpressionOperation.And, procedurDefId, "Date", ConditionOperation.LessEqual, ld);
                query.AddCondition(ExpressionOperation.And, iprDefId, "IndividualRehabilitationPlan", ConditionOperation.Equal, iprId);
                query.AddAttribute(appSrc, "&Id");
                var list = new List<Guid>();
                int count = 0;
                var table = new DataTable();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }
                foreach (DataRow row in table.Rows)
                {
                    var appId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                    if (appId != Guid.Empty && !list.Contains(appId))
                    {
                        list.Add(appId);
                        count++;
                    }
                }
                return count;
            }

            public class ReportItem
            {
                public Guid FieldName { get; set; }
                public string FieldNameText { get; set; }

                public string StringCount { get; set; }
                public int Men { get; set; }
                public int Women { get; set; }
            }
        } //old
        public static class PatientCountOfMonth_6001new
        {
            public static List<ReportItem> Execute(WorkflowContext context, DateTime fd, DateTime ld)
            {
                var ui = context.GetUserInfo();
                var orgId = ui.OrganizationId.Value;

                var docs = new string[]
                {
                    "Переходящие с предыдущей недели/месяца",
                    "Сопровождающие (переходящие с предыдущей недели/месяца)",
                    "Находились в стационаре",
                    "Всего проведено койко дней",
                    "Из них ЛОВЗ всего",
                    "Проведено ЛОВЗ койко дней",
                    "Сопровождающие",
                    "Койко дней сопровождающих",
                    "Госпитализ на протез и реабил. всего",
                    "Койко-дней на реабилитацию с протезированием",
                    "Госпитализ на реабилитацию всего",
                    "Койка-дни на реабилитацию",
                    "Первичное протезирование и реабилитация",
                    "Первичная реабилитация",
                    "ИПР",
                    "За отчетный период поступили всего",
                    "Сопровождающие",
                    "Дети с ОВЗ",
                    "ЛОВЗ I группы",
                    "ЛОВЗ III группы",
                    "ЛОВЗ II группы",
                    "Медикаментозная реабилитация",
                    "Проведено физиопроцедур",
                    "Парафинотерапия",
                    "Перевязок",
                    "Массаж",
                    "Проведено ЛФК",
                    "Оказано стом. помощь",
                    "Проведено зубопротезирований",
                    "Медико-техническая реабилитация",
                    "Психологическая реабилитация",
                    "ЭКГ",
                    "УЗДГ",
                    "УЗИ",
                    "Обучение базовым знаниям ПК",
                    "Обучение мелкому ремонту обуви",
                    "Обучение шитью на шв. машинке и бисероплет. и.т.п.",
                    "Поступили по направлению мед.отдела/РУПОИ",
                    "Поступили по направлению МСЭ",
                    "Поступили по направлению ЦСМ/ГСВ",
                    "Поступили по направлению ССУ",
                    "Выписано ЛОВЗ за отчетный период",
                    "Выписано сопровождающих",
                    "Хороший результат",
                    "Удовлетворительный результат",
                    "Без изменений",
                    "Чуйская область",
                    "Иссык-Кульская область",
                    "Таласская область",
                    "Нарынская область",
                    "Жалал-Абадская область",
                    "Ошская область",
                    "Баткенская область",
                    "г. Каракол",
                    "г. Балыкчы",
                    "г. Чолпон-Ата",
                    "г. Кербен",
                    "г. Кочкор-Ата",
                    "г. Кок-Жангак",
                    "г. Токтогул",
                    "г. Джалал-Абад",
                    "г. Таш-Кумыр",
                    "Кызыл-Джар",
                    "г. Шамалды-Сай",
                    "г.  Майлуу-Суу",
                    "Кек-Таш",
                    "Кара-Куль",
                    "Кетмен-Тебе",
                    "г. Нарын",
                    "г. Талас",
                    "г. Баткен",
                    "г. Исфана",
                    "Айдаркен",
                    "Ортотокой",
                    "Кадамжай",
                    "Сулюкта",
                    "Восточный",
                    "Кызыл-Кия",
                    "Кара-Суу",
                    "Ноокат",
                    "Узген",
                    "Кант",
                    "Кара-Балта",
                    "Орловка",
                    "Кемин",
                    "Бордунский",
                    "Каинды",
                    "Шопоков",
                    "Токмок",
                    "г. Ош",
                    "Акталинский район",
                    "Атбашинский район",
                    "Нарынский район",
                    "Жумгальский район",
                    "Кочкорский район",
                    "Аксуйский район",
                    "Джети-Огузский район",
                    "Иссык-Кульский район",
                    "Тонский район",
                    "Тюпский район",
                    "Алабукинский район",
                    "Базар-Коргонский район",
                    "Аксыйский район",
                    "Ноокенский район",
                    "Сузакский район",
                    "Тогуз-Тороуский район",
                    "Токтогульский район",
                    "Чаткальский район",
                    "Баткенский район",
                    "Лейлекский район",
                    "Кадамжайский район",
                    "Алайский район",
                    "Араванский район",
                    "Карасуйский район",
                    "Ноокатский район",
                    "Кара-Кульджинский район",
                    "Узгенский район",
                    "Чоналайский район",
                    "Карабууринский район",
                    "Бакай-Атинский район",
                    "Манасский район",
                    "Таласский район",
                    "Аламудунский район",
                    "Ысык-Атинский район",
                    "Жайылский район",
                    "Кеминский район",
                    "Московский район",
                    "Панфиловский район",
                    "Сокулукский район",
                    "Чуйский район",
                    "Ленинский район",
                    "Первомайский район",
                    "Свердловский район",
                    "Октябрьский район"
                };
                var items = new List<ReportItem>();
                int i2 = 1;
                foreach (var i in docs)
                {
                    var item = new ReportItem();
                    item.rowName = i;
                    item.rowNo = i2;
                    i2++;
                    items.Add(item);
                }
                CalcItems(context, orgId, fd, ld,
                    items[133], items[132], items[131], items[130], items[129], items[128], items[127], items[126], items[125],
                    items[124], items[123], items[122], items[121], items[120], items[119], items[118], items[117], items[116], items[115],
                    items[114], items[113], items[112], items[111], items[110], items[109], items[108], items[107], items[106], items[105], items[104],
                    items[103], items[102], items[101], items[100], items[99], items[98], items[97], items[96], items[95], items[94], items[93], items[92],
                    items[91], items[90], items[89], items[88], items[87], items[86], items[85], items[84], items[83], items[82], items[81], items[80],
                    items[79], items[78], items[77], items[76], items[75], items[74], items[73], items[72], items[71], items[70], items[69], items[68],
                    items[67], items[66], items[65], items[64], items[63], items[62], items[61], items[60], items[59], items[58], items[57], items[56],
                    items[55], items[54], items[53], items[52], items[51], items[50], items[49], items[48], items[47], items[46], items[45], items[44],
                    items[43], items[42], items[41], items[40], items[39], items[38], items[37], items[36], items[35], items[34], items[33], items[32],
                    items[31], items[30], items[29], items[28], items[27], items[26], items[25], items[24], items[23], items[22], items[21], items[20],
                    items[19], items[18], items[17], items[16], items[15], items[14], items[13], items[12], items[11], items[10], items[9], items[8],
                    items[7], items[6], items[5], items[4], items[3], items[2], items[1], items[0]);
                return items;
            }
            public static void CalcItems(WorkflowContext context, Guid orgId, DateTime fd, DateTime ld,
               ReportItem item133, ReportItem item132, ReportItem item131, ReportItem item130, ReportItem item129, ReportItem item128, ReportItem item127,
               ReportItem item126, ReportItem item125, ReportItem item124, ReportItem item123, ReportItem item122, ReportItem item121, ReportItem item120,
               ReportItem item119, ReportItem item118, ReportItem item117, ReportItem item116, ReportItem item115, ReportItem item114, ReportItem item113,
               ReportItem item112, ReportItem item111, ReportItem item110, ReportItem item109, ReportItem item108, ReportItem item107, ReportItem item106,
               ReportItem item105, ReportItem item104, ReportItem item103, ReportItem item102, ReportItem item101, ReportItem item100, ReportItem item99,
               ReportItem item98, ReportItem item97, ReportItem item96, ReportItem item95, ReportItem item94, ReportItem item93, ReportItem item92,
               ReportItem item91, ReportItem item90, ReportItem item89, ReportItem item88, ReportItem item87, ReportItem item86, ReportItem item85,
               ReportItem item84, ReportItem item83, ReportItem item82, ReportItem item81, ReportItem item80, ReportItem item79, ReportItem item78,
               ReportItem item77, ReportItem item76, ReportItem item75, ReportItem item74, ReportItem item73, ReportItem item72, ReportItem item71,
               ReportItem item70, ReportItem item69, ReportItem item68, ReportItem item67, ReportItem item66, ReportItem item65, ReportItem item64,
               ReportItem item63, ReportItem item62, ReportItem item61, ReportItem item60, ReportItem item59, ReportItem item58, ReportItem item57,
               ReportItem item56, ReportItem item55, ReportItem item54, ReportItem item53, ReportItem item52, ReportItem item51, ReportItem item50,
               ReportItem item49, ReportItem item48, ReportItem item47, ReportItem item46, ReportItem item45, ReportItem item44, ReportItem item43,
               ReportItem item42, ReportItem item41, ReportItem item40, ReportItem item39, ReportItem item38, ReportItem item37, ReportItem item36,
               ReportItem item35, ReportItem item34, ReportItem item33, ReportItem item32, ReportItem item31, ReportItem item30, ReportItem item29,
               ReportItem item28, ReportItem item27, ReportItem item26, ReportItem item25, ReportItem item24, ReportItem item23, ReportItem item22,
               ReportItem item21, ReportItem item20, ReportItem item19, ReportItem item18, ReportItem item17, ReportItem item16, ReportItem item15,
               ReportItem item14, ReportItem item13, ReportItem item12, ReportItem item11, ReportItem item10, ReportItem item9, ReportItem item8,
               ReportItem item7, ReportItem item6, ReportItem item5, ReportItem item4, ReportItem item3, ReportItem item2, ReportItem item1, ReportItem item0)
            {
                var qb = new QueryBuilder(statCartDefId, context.UserId);

                using (var query = SqlQueryBuilder.Build(context.DataContext, qb.Def))
                {
                    query.AddCondition(ExpressionOperation.And, statCartDefId, "&OrgId", ConditionOperation.Equal, orgId);

                    query.AddAttribute(query.Source, "&Id");
                    query.AddAttribute(query.Source, "Obl");
                    query.AddAttribute(query.Source, "Rag");

                    var table = new DataTable();
                    using (var reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }

                    foreach (DataRow row in table.Rows)
                    {
                        var cardId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                        var area = row[1] is DBNull ? Guid.Empty : (Guid)row[1];
                        var district = row[2] is DBNull ? Guid.Empty : (Guid)row[2];

                        if (cardId != Guid.Empty)
                        {
                            if (area != Guid.Empty)
                            {
                                if ((area.Equals(new Guid("{0504E82A-7468-4DAF-BCA1-F31163241C02}"))) || (area.Equals(new Guid("{FCB6DB01-469E-4254-9BB6-E3409057A779}"))))
                                    item46.gr1 = item46.gr1 != string.Empty ? (Convert.ToInt32(item46.gr1) + 1).ToString() : "1";
                                if (area.Equals(new Guid("{4386D93C-2DF6-4F99-A438-55C20447FD24}")))
                                    item47.gr1 = item47.gr1 != string.Empty ? (Convert.ToInt32(item47.gr1) + 1).ToString() : "1";
                                if (area.Equals(new Guid("{F2662FF6-FFA7-4707-8284-3FB52436C1F1}")))
                                    item48.gr1 = item48.gr1 != string.Empty ? (Convert.ToInt32(item48.gr1) + 1).ToString() : "1";
                                if (area.Equals(new Guid("{591191B0-7827-4C47-AC47-FB03C685E911}")))
                                    item49.gr1 = item49.gr1 != string.Empty ? (Convert.ToInt32(item49.gr1) + 1).ToString() : "1";
                                if (area.Equals(new Guid("{65C856CD-69E5-4CA4-BB8C-8F382AE659E8}")))
                                    item50.gr1 = item50.gr1 != string.Empty ? (Convert.ToInt32(item50.gr1) + 1).ToString() : "1";
                                if ((area.Equals(new Guid("{92267258-B11F-4B11-B781-2953F9E064A7}"))) || (area.Equals(new Guid("{D1067965-ACC3-4C43-9CA3-DC50C0E4D128}"))))
                                    item51.gr1 = item51.gr1 != string.Empty ? (Convert.ToInt32(item51.gr1) + 1).ToString() : "1";
                                if (area.Equals(new Guid("{4171C264-3F39-4B23-AAAC-B4282C68C6A6}")))
                                    item52.gr1 = item52.gr1 != string.Empty ? (Convert.ToInt32(item52.gr1) + 1).ToString() : "1";
                            }
                            if (district != Guid.Empty)
                            {
                                if (district.Equals(new Guid("{79AAD00B-DC3A-4247-BCF7-CDE467B96E4F}"))) item53.gr1 = item53.gr1 != string.Empty ? (Convert.ToInt32(item53.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{F3D9FBB2-D7CF-4CF9-B085-7D0793B37370}"))) item54.gr1 = item54.gr1 != string.Empty ? (Convert.ToInt32(item54.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{6674AFF4-B55C-45E3-A487-249317B33DF4}"))) item55.gr1 = item55.gr1 != string.Empty ? (Convert.ToInt32(item55.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{CA401F89-C9F5-4CEF-A7CF-2A0FDFEDD5FF}"))) item56.gr1 = item56.gr1 != string.Empty ? (Convert.ToInt32(item56.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{729FD82B-B855-4925-BBA6-6D450889D3F9}"))) item57.gr1 = item57.gr1 != string.Empty ? (Convert.ToInt32(item57.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{42D826F0-4AAB-4F4F-B08D-191ADDF55743}"))) item58.gr1 = item58.gr1 != string.Empty ? (Convert.ToInt32(item58.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{5579EDE7-40E8-4092-A6CF-30D96803637C}"))) item59.gr1 = item59.gr1 != string.Empty ? (Convert.ToInt32(item59.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{C9F27576-91E7-4A99-856E-38BBA988AF22}"))) item60.gr1 = item60.gr1 != string.Empty ? (Convert.ToInt32(item60.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{767CB4C4-C05F-4229-8960-6604744FFF0F}"))) item61.gr1 = item61.gr1 != string.Empty ? (Convert.ToInt32(item61.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{D9E97D93-0CE2-421F-87A9-3FF1554D3BBC}"))) item62.gr1 = item62.gr1 != string.Empty ? (Convert.ToInt32(item62.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{7497BA2E-F5F0-4663-B337-2E8966D1A919}"))) item63.gr1 = item63.gr1 != string.Empty ? (Convert.ToInt32(item63.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{5208308E-2561-4CC2-8BB5-88584F615E71}"))) item64.gr1 = item64.gr1 != string.Empty ? (Convert.ToInt32(item64.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{03B2DB0C-12B7-44F8-AF9F-E51DA49F7D57}"))) item65.gr1 = item65.gr1 != string.Empty ? (Convert.ToInt32(item65.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{15895A37-560D-4AF0-A971-05EB7C19AD58}"))) item66.gr1 = item66.gr1 != string.Empty ? (Convert.ToInt32(item66.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{F8041D96-55D9-4840-B175-12CF10529AFF}"))) item67.gr1 = item67.gr1 != string.Empty ? (Convert.ToInt32(item67.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{8369FB92-2EB4-43E7-B9D1-AAD62A06D307}"))) item68.gr1 = item68.gr1 != string.Empty ? (Convert.ToInt32(item68.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{85706905-A4E4-4DED-973D-EC1C09776746}"))) item69.gr1 = item69.gr1 != string.Empty ? (Convert.ToInt32(item69.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{4DB17EAE-4381-4E0E-94CD-108E0F421E36}"))) item70.gr1 = item70.gr1 != string.Empty ? (Convert.ToInt32(item70.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{E28660FF-2877-472B-BA70-1A89F5E8A8E6}"))) item71.gr1 = item71.gr1 != string.Empty ? (Convert.ToInt32(item71.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{601E1AB1-FD5A-42DE-BE36-6FA265CE0C59}"))) item72.gr1 = item72.gr1 != string.Empty ? (Convert.ToInt32(item72.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{1E549247-6C19-4ED6-A013-E028B4EE0887}"))) item73.gr1 = item73.gr1 != string.Empty ? (Convert.ToInt32(item73.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{5C8C633B-125F-4CDF-902C-574CF78A6728}"))) item74.gr1 = item74.gr1 != string.Empty ? (Convert.ToInt32(item74.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{CB9A187F-4E16-41BC-A745-3DB9ADEB1B02}"))) item75.gr1 = item75.gr1 != string.Empty ? (Convert.ToInt32(item75.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{8B5C6DA6-4283-4D8B-AD37-153874EDF0AD}"))) item76.gr1 = item76.gr1 != string.Empty ? (Convert.ToInt32(item76.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{7D735007-FDB7-48EA-A9E8-988BE0D40C96}"))) item77.gr1 = item77.gr1 != string.Empty ? (Convert.ToInt32(item77.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{57FD5C59-1C04-478B-99AB-BB330DF8B2EE}"))) item78.gr1 = item78.gr1 != string.Empty ? (Convert.ToInt32(item78.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{8497A553-07EE-4146-86BA-ED714C8E2D2B}"))) item79.gr1 = item79.gr1 != string.Empty ? (Convert.ToInt32(item79.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{156E9D8B-008E-4A09-8142-96CEC1AEA1F7}"))) item80.gr1 = item80.gr1 != string.Empty ? (Convert.ToInt32(item80.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{5AC16B2B-9A3E-4797-9CDE-30C5B6875B2F}"))) item81.gr1 = item81.gr1 != string.Empty ? (Convert.ToInt32(item81.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{446AB760-4952-480A-A2C8-C6E55E530306}"))) item82.gr1 = item82.gr1 != string.Empty ? (Convert.ToInt32(item82.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{F523CE1A-E8AA-4C28-B54A-CDBB9FDA06EE}"))) item83.gr1 = item83.gr1 != string.Empty ? (Convert.ToInt32(item83.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{B83B35FC-8DE8-4531-A55B-2122B6927BB7}"))) item84.gr1 = item84.gr1 != string.Empty ? (Convert.ToInt32(item84.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{7241D184-10D5-44EA-9696-FB17CC54908D}"))) item85.gr1 = item85.gr1 != string.Empty ? (Convert.ToInt32(item85.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{0BB1ED87-390A-4E78-9C39-43548000050B}"))) item86.gr1 = item86.gr1 != string.Empty ? (Convert.ToInt32(item86.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{000A1444-070E-4E88-B721-A124286CDF52}"))) item87.gr1 = item87.gr1 != string.Empty ? (Convert.ToInt32(item87.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{C028DF94-6F08-427B-B7E3-144E288AC8DF}"))) item88.gr1 = item88.gr1 != string.Empty ? (Convert.ToInt32(item88.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{CDD62071-0103-4AEA-9716-F878A8EC794A}"))) item89.gr1 = item89.gr1 != string.Empty ? (Convert.ToInt32(item89.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{44AC559E-9176-471D-9625-B4F5A8D4C017}"))) item90.gr1 = item90.gr1 != string.Empty ? (Convert.ToInt32(item90.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{29DD2C55-E331-4BDF-BFFB-8BAF01CE38A4}"))) item91.gr1 = item91.gr1 != string.Empty ? (Convert.ToInt32(item91.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{7BB62C60-E8D6-4CA2-88A3-9449E5F15EE9}"))) item92.gr1 = item92.gr1 != string.Empty ? (Convert.ToInt32(item92.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{7EC8E15D-FADD-4973-8253-D7CE8A3F8B7A}"))) item93.gr1 = item93.gr1 != string.Empty ? (Convert.ToInt32(item93.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{3A94B34F-5924-471E-BAE8-D30AB49B1DE9}"))) item94.gr1 = item94.gr1 != string.Empty ? (Convert.ToInt32(item94.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{7C08DEDF-4C83-4ACE-BACB-87683D7FE9AA}"))) item95.gr1 = item95.gr1 != string.Empty ? (Convert.ToInt32(item95.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{DD00A50F-CB35-4080-AE7B-A5C26BE9C0A9}"))) item96.gr1 = item96.gr1 != string.Empty ? (Convert.ToInt32(item96.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{0DE01029-2BF3-4042-83DA-5633C397C2A2}"))) item97.gr1 = item97.gr1 != string.Empty ? (Convert.ToInt32(item97.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{940A7FA2-A4D6-401F-AF8A-8DD67DEA2295}"))) item98.gr1 = item98.gr1 != string.Empty ? (Convert.ToInt32(item98.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{2FF3A76E-F7C6-4F44-8F91-192B431A1106}"))) item99.gr1 = item99.gr1 != string.Empty ? (Convert.ToInt32(item99.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{C8F6A8BF-F51F-427A-AA53-EA7CD1B63C7C}"))) item100.gr1 = item100.gr1 != string.Empty ? (Convert.ToInt32(item100.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{04EB0012-E8E7-4DA6-9BA5-28ED0F8BBE69}"))) item101.gr1 = item101.gr1 != string.Empty ? (Convert.ToInt32(item101.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{0648E502-3EA2-4FD6-B505-78331D7F337D}"))) item102.gr1 = item102.gr1 != string.Empty ? (Convert.ToInt32(item102.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{61806A08-B216-497E-889F-C5FFB2DDF4F2}"))) item103.gr1 = item103.gr1 != string.Empty ? (Convert.ToInt32(item103.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{358379BD-F7CB-43BF-B0D5-3E4DF0EBC586}"))) item104.gr1 = item104.gr1 != string.Empty ? (Convert.ToInt32(item104.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{073217E4-BD96-4EA5-BC1B-777D2E248294}"))) item105.gr1 = item105.gr1 != string.Empty ? (Convert.ToInt32(item105.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{711DEA4C-C051-4B8E-AF89-143326DBDDCD}"))) item106.gr1 = item106.gr1 != string.Empty ? (Convert.ToInt32(item106.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{091A243B-2565-45D7-B02D-6FF2E2A02A1D}"))) item107.gr1 = item107.gr1 != string.Empty ? (Convert.ToInt32(item107.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{76BC0B28-12D5-4502-B072-3940D0949B7E}"))) item108.gr1 = item108.gr1 != string.Empty ? (Convert.ToInt32(item108.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{42C3CB7B-37CC-4D40-9BB3-2B30FBE07D0F}"))) item109.gr1 = item109.gr1 != string.Empty ? (Convert.ToInt32(item109.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{26202293-B601-4F4B-9142-FDDEFB8063E6}"))) item110.gr1 = item110.gr1 != string.Empty ? (Convert.ToInt32(item110.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{CDB05FCA-44EA-4713-99EC-9E1D80271FEE}"))) item111.gr1 = item111.gr1 != string.Empty ? (Convert.ToInt32(item111.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{E491CD42-EA35-417D-8C1B-59287BD2D8E8}"))) item112.gr1 = item112.gr1 != string.Empty ? (Convert.ToInt32(item112.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{F50FD5B9-16F3-45AC-BAEA-02640F02912B}"))) item113.gr1 = item113.gr1 != string.Empty ? (Convert.ToInt32(item113.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{FCB537D1-6726-40E2-B484-91CE9C698895}"))) item114.gr1 = item114.gr1 != string.Empty ? (Convert.ToInt32(item114.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{4E958C6E-E319-4211-B047-F56EB69EF4CC}"))) item115.gr1 = item115.gr1 != string.Empty ? (Convert.ToInt32(item115.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{CA7BCCC5-EEC2-4C1A-A1AB-39AC3913E296}"))) item116.gr1 = item116.gr1 != string.Empty ? (Convert.ToInt32(item116.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{FA27FF69-6B10-4A6C-8A5C-9E6447C5604B}"))) item117.gr1 = item117.gr1 != string.Empty ? (Convert.ToInt32(item117.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{2AB3580D-1B0E-4824-986D-A14D2AE6D281}"))) item118.gr1 = item118.gr1 != string.Empty ? (Convert.ToInt32(item118.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{8D9BC28D-3177-4663-8F26-1F6F5C7FCF42}"))) item119.gr1 = item119.gr1 != string.Empty ? (Convert.ToInt32(item119.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{D0865B0B-793D-438C-9D53-A71FF4D599F1}"))) item120.gr1 = item120.gr1 != string.Empty ? (Convert.ToInt32(item120.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{B1A29E3B-3F80-49DF-A94F-6F3F91165112}"))) item121.gr1 = item121.gr1 != string.Empty ? (Convert.ToInt32(item121.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{8DDDADBF-710E-4182-A912-C98604080BAF}"))) item122.gr1 = item122.gr1 != string.Empty ? (Convert.ToInt32(item122.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{4ED420A7-1561-417B-AC49-B0123A6A0E03}"))) item123.gr1 = item123.gr1 != string.Empty ? (Convert.ToInt32(item123.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{0E5917DA-8BFE-4E50-819D-065AF47B5CAD}"))) item124.gr1 = item124.gr1 != string.Empty ? (Convert.ToInt32(item124.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{609F1D57-023D-400F-B3FE-F8EE9D20BEB9}"))) item125.gr1 = item125.gr1 != string.Empty ? (Convert.ToInt32(item125.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{18988DC4-FFD1-4DA5-BB90-5738F3BA08BF}"))) item126.gr1 = item126.gr1 != string.Empty ? (Convert.ToInt32(item126.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{6C7FF2AC-5394-4F7F-A085-709A7D0DCB70}"))) item127.gr1 = item127.gr1 != string.Empty ? (Convert.ToInt32(item127.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{3642C666-F83E-43C2-9D6E-29C9E527E22A}"))) item128.gr1 = item128.gr1 != string.Empty ? (Convert.ToInt32(item128.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{80AD3EBC-43A3-43D3-85FB-3DE5C2DA5364}"))) item129.gr1 = item129.gr1 != string.Empty ? (Convert.ToInt32(item129.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{03C74B83-32EC-48AC-B4C8-E4EAF24D089B}"))) item130.gr1 = item130.gr1 != string.Empty ? (Convert.ToInt32(item130.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{3D17C5D9-315E-4D23-8352-3367C0FDAF88}"))) item131.gr1 = item131.gr1 != string.Empty ? (Convert.ToInt32(item131.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{09824519-A477-4901-AA1C-7C97BE816B9A}"))) item132.gr1 = item132.gr1 != string.Empty ? (Convert.ToInt32(item132.gr1) + 1).ToString() : "1";
                                if (district.Equals(new Guid("{DF466856-AB99-4C6B-99B8-A0172FED7964}"))) item133.gr1 = item133.gr1 != string.Empty ? (Convert.ToInt32(item133.gr1) + 1).ToString() : "1";
                            }
                        }
                    }
                }
            }
            private static readonly Guid statCartDefId = new Guid("{683B63E2-B0C6-470F-8A01-5D73AB145F8A}"); //стат карта.
            private static readonly Guid personDefId = new Guid("{6F5B8A06-361E-4559-8A53-9CB480A9B16C}"); //person
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}");  //Утвержденные
            public class ReportItem
            {
                public int rowNo { get; set; }
                public string rowName { get; set; }
                public string gr1 { get; set; }
            }
        }

        public static class FC_1024
        {
            private static readonly Guid uyBulogoKomokPaymentId = new Guid("{3193A90D-380B-4428-B7B1-04C548AA902E}");

            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{1A09ECD6-55E1-4307-862E-6F98F47E252C}");
            private static readonly Guid reportItemDefId = new Guid("{2A8709AB-3522-4019-A29F-5C333893645B}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid postDefId = new Guid("{AC649550-67AF-4F7F-8D96-DC22484F7F04}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");

            private static readonly Guid retirementBenefitPaymentId = new Guid("{AFAA7F86-74AE-4260-9933-A56F7845E55A}");  // ЕСП престарелым гражданам
            private static readonly Guid childBenefitPaymentId = new Guid("{AB3F8C41-897A-4574-BAA0-B7CD4AAA1C80}");  // ЕСП на инвалида - члена семьи
            private static readonly Guid invalidBenefitPaymentId = new Guid("{70C28E62-2387-4A59-917D-A366ADE119A8}");  // ЕСП по инвалидности
            private static readonly Guid survivorBenefitPaymentId = new Guid("{839D5712-E75B-4E71-83F7-168CE4F089C0}");  // ЕСП детям при утере кормильца 
            private static readonly Guid aidsFromBenefitPaymentId = new Guid("{3BEBE4F9-0B15-41CB-9B96-54E83819AB0F}");  // ЕСП детям, инфецированным ВИЧ/СПИД
            private static readonly Guid aidsBenefitPaymentId = new Guid("{47EEBFBC-A4E9-495D-A6A1-F87B5C3057C9}");  // ЕСП детям от матерей с ВИЧ/СПИД до 18 месяцев
            private static readonly Guid orphanBenefitPaymentId = new Guid("{4F12C366-7E2F-4208-9CB8-4EAB6E6C0EF1}");   // ЕСП круглым сиротам 

            // States                               
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid canceledStateId = new Guid("{C65E63CC-54E9-424E-AFDC-5A8DB1CB04A3}"); // Аннулирован
            private static readonly Guid onApprovingStateId = new Guid("{5CD9E88D-671E-4A44-AD92-9F74DA3B47F7}"); //На утверждении
            private static readonly Guid onRegisteringStateId = new Guid("{A9CD37C4-A718-4DE1-9E95-EC8EC280C8D4}"); // На регистрации
            private static readonly Guid refusedStateId = new Guid("{CA1157A0-FDDF-4C90-9692-7CDB47CCC7C2}"); // Отказан
            private static readonly Guid onPaymentStateTypeId = new Guid("{3DB4DB00-3A7F-4228-A9A3-A413B85C18B4}"); //Оформлен

            private static readonly Guid childUpTo3Id = new Guid("{D8FF3DAF-A701-414A-B965-4BF93BB658B9}"); //Ребенок до 3-х лет
            private static readonly Guid childUpTo16Id = new Guid("{8024982A-4AFB-4074-9D63-3EEFE22420E0}");  //Ребенок от 3-х до 16 лет  
            private static readonly Guid childUpTo1618Id = new Guid("{8024982A-4AFB-4074-9D63-3EEFE22420E0}");  //Учащийся 16-18л

            private static readonly Guid childISPCategoryId = new Guid("{1E750C67-2DDF-488E-A4C4-D94547433067}"); //Дети, больные ДЦП
            private static readonly Guid childDisableCategoryId = new Guid("{FDBDD774-EB88-46EA-9559-005D655BC196}");//ЛОВЗ (до 18 лет)
            private static readonly Guid childenAidsCategoryId = new Guid("{2222FD98-B885-4DC0-A0D4-271600AF281A}"); //Дети ВИЧ-инфиц.или больные СПИДом
            private static readonly Guid childFromAidsCategoryId = new Guid("{DE66F6D7-5462-4D45-8B20-6478728B5BD3}"); //Дети, рожденные от матерей,живущих с ВИЧ-СПИДом
            private static readonly Guid childhood1CategoryId = new Guid("{D18791C9-DE0A-4E15-92A8-20EF140C51ED}");
            private static readonly Guid childhood2CategoryId = new Guid("{305621EC-9ECC-4AF9-810D-5B639C339D50}");
            private static readonly Guid childhood3CategoryId = new Guid("{FD3B12FB-55D3-4229-975E-342AC126E942}");
            private static readonly Guid commonDeseas1CategoryId = new Guid("{409BCDA9-6770-4D3F-B515-7DE0E341C63D}");
            private static readonly Guid commonDeseas2CategoryId = new Guid("{0955ED04-8A32-476B-AE6E-E51DE0F2C66D}");
            private static readonly Guid commonDeseas3CategoryId = new Guid("{7B622DDA-D6C0-48CA-AFA9-7C74149D8BD5}");
            private static readonly Guid alpineRetirementCategoryId = new Guid("{587D9992-DBB7-4BAD-A358-0FA571EBDB37}");
            private static readonly Guid retirementCategoryId = new Guid("{56DD1E0D-F693-470D-8756-6969DFA71A02}");
            private static readonly Guid heroMotherCategoryId = new Guid("{F21D59E6-BBAA-40D8-89FC-C7B3A707E8E6}");
            private static readonly Guid orphanChildren = new Guid("{401D9570-2A4E-453D-869E-9AA2603C9CD8}");
            private static readonly Guid childSurvivorsCategoryId = new Guid("{304A01B8-8A08-413E-B3D5-7B2C237829A2}");
            private static readonly List<Guid> childUpTo18YearList = new List<Guid>
            {
                childUpTo3Id,
                childUpTo16Id,
                childUpTo1618Id
            };
            private static readonly List<Guid> msbList = new List<Guid>
            {
                    retirementBenefitPaymentId,
                    childBenefitPaymentId,
                    survivorBenefitPaymentId,
                    aidsFromBenefitPaymentId,
                    aidsBenefitPaymentId,
                    orphanBenefitPaymentId
            };
            private static readonly Guid twinsId = new Guid("{45D55628-5E72-42B8-8B8D-667346E79046}"); // Двойня
            private static readonly Guid tripletsId = new Guid("{8CCAE21E-128A-4728-9479-9C094271C614}"); // Тройня

            private static Guid balagaSuyunchuApp = new Guid("{4B8EA3F0-D536-4A59-9D2A-41872D63103C}");
            private static Guid payable_state = new Guid("{3DB4DB00-3A7F-4228-A9A3-A413B85C18B4}");

            private static readonly Guid applicantId = new Guid("{6F5B8A06-361E-4559-8A53-9CB480A9B16C}");
            private static readonly Guid childId = new Guid("{6F5B8A06-361E-4559-8A53-9CB480A9B16C}");
            private static readonly Guid bankDetailsId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            public class ReportItem
            {
                public ReportItem()
                {
                    allReceiverUBK = new SubItem();
                    allFamilyUBK = new SubItem();
                    allReceiverBalagaSuyunchu = new SubItem();
                    disabledChildCerebralPalsy = new SubItem();
                    disabledChildUpTo18Year = new SubItem();
                    disabledChildAIDS = new SubItem();
                    disabledChildBurnFromAIDSMothers = new SubItem();
                    disabledChildUpTo18YearAll = new SubItem();
                    disabledChildFirstCategory = new SubItem();
                    disabledChildSecondCategory = new SubItem();
                    disabledChildThirdCategory = new SubItem();
                    disabledChildWithCategoriesAll = new SubItem();
                    disabledGeneralIlnessFirstCategory = new SubItem();
                    disabledGeneralIlnessSecondCategory = new SubItem();
                    disabledGeneralIlnessThirdCategory = new SubItem();
                    disabledGeneralIlnessWithCategoriesAll = new SubItem();
                    agedCitizens = new SubItem();
                    agedHighlandResident = new SubItem();
                    motherHeroine = new SubItem();
                    childLoseBreadWinner = new SubItem();
                    childOrphan = new SubItem();

                }
                public SubItem allReceiverUBK { get; set; }
                public decimal averageSumPriceUBK { get; set; }
                public SubItem allFamilyUBK { get; set; }
                public SubItem allReceiverBalagaSuyunchu { get; set; }
                public SubItem disabledChildCerebralPalsy { get; set; }
                public SubItem disabledChildUpTo18Year { get; set; }
                public SubItem disabledChildAIDS { get; set; }
                public SubItem disabledChildBurnFromAIDSMothers { get; set; }
                public SubItem disabledChildUpTo18YearAll { get; set; }

                public SubItem disabledChildFirstCategory { get; set; }
                public SubItem disabledChildSecondCategory { get; set; }
                public SubItem disabledChildThirdCategory { get; set; }
                public SubItem disabledChildWithCategoriesAll { get; set; }

                public SubItem disabledGeneralIlnessFirstCategory { get; set; }
                public SubItem disabledGeneralIlnessSecondCategory { get; set; }
                public SubItem disabledGeneralIlnessThirdCategory { get; set; }
                public SubItem disabledGeneralIlnessWithCategoriesAll { get; set; }

                public SubItem agedCitizens { get; set; }
                public SubItem agedHighlandResident { get; set; }
                public SubItem motherHeroine { get; set; }
                public SubItem childLoseBreadWinner { get; set; }
                public SubItem childOrphan { get; set; }
            }

            public class SubItem
            {
                public int Count { get; set; }
                public decimal SumOfPrice { get; set; }
            }
            public static ReportItem Execute(WorkflowContext context, int year, int month)
            {
                var item = new ReportItem();
                var fd = new DateTime(year, month, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var qb = new QueryBuilder(appDefId, context.UserId);
                var tempList = new HashSet<Guid>();
                qb.Where("Assignments").Include("PaymentType")
                    .In(new object[]
                    {
                    uyBulogoKomokPaymentId,
                    retirementBenefitPaymentId,
                    childBenefitPaymentId,
                    survivorBenefitPaymentId,
                    aidsFromBenefitPaymentId,
                    aidsBenefitPaymentId,
                    orphanBenefitPaymentId
                    })
                    .And("EffectiveDate").Lt(ld).And("ExpiryDate").Gt(fd).End()
                     .And("&State")
                     .In(new object[]
                    {
                        approvedStateId,
                        onApprovingStateId,
                        onPaymentStateTypeId
                    });
                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var assignmentsSrc = query.JoinSource(query.Source, assignmentDefId, SqlSourceJoinType.Inner, "Assignments");

                query.AddAttribute(query.Source, "&Id");
                query.AddAttribute(assignmentsSrc, "Amount");
                query.AddAttribute(assignmentsSrc, "Category");
                query.AddAttribute(assignmentsSrc, "PaymentType");
                query.AddAttribute(assignmentsSrc, "EmploymentStatus");
                query.AddAttribute(assignmentsSrc, "MembershipType");

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        var appId = reader.IsDbNull(0) ? Guid.Empty : reader.GetGuid(0);
                        if (appId == Guid.Empty) continue;
                        var sum = reader.IsDbNull(1) ? 0m : reader.GetDecimal(1);
                        var categoryId = reader.IsDbNull(2) ? Guid.Empty : reader.GetGuid(2);
                        var paymentTypeId = reader.IsDbNull(3) ? Guid.Empty : reader.GetGuid(3);
                        var employmentStatusId = reader.IsDbNull(4) ? Guid.Empty : reader.GetGuid(4);
                        var membershipTypeId = reader.IsDbNull(5) ? Guid.Empty : reader.GetGuid(5);

                        if (paymentTypeId.Equals(uyBulogoKomokPaymentId))
                        {
                            item.allReceiverUBK.Count += 1;
                            item.allReceiverUBK.SumOfPrice = sum;
                            item.averageSumPriceUBK += sum;
                            if (!tempList.Contains(appId))
                            {
                                item.allFamilyUBK.Count += 1;
                                tempList.Add(appId);
                            }
                        }
                        if (msbList.Contains(paymentTypeId))
                        {
                            if (categoryId.Equals(childISPCategoryId))
                            {
                                item.disabledChildCerebralPalsy.Count += 1;
                                item.disabledChildCerebralPalsy.SumOfPrice = sum;
                            }
                            if (categoryId.Equals(childDisableCategoryId))
                            {
                                item.disabledChildUpTo18Year.Count += 1;
                                item.disabledChildUpTo18Year.SumOfPrice = sum;
                            }
                            if (categoryId.Equals(childenAidsCategoryId))
                            {
                                item.disabledChildAIDS.Count += 1;
                                item.disabledChildAIDS.SumOfPrice = sum;
                            }
                            if (categoryId.Equals(childFromAidsCategoryId))
                            {
                                item.disabledChildBurnFromAIDSMothers.Count += 1;
                                item.disabledChildBurnFromAIDSMothers.SumOfPrice = sum;
                            }
                            if (categoryId.Equals(childhood1CategoryId))
                            {
                                item.disabledChildFirstCategory.Count += 1;
                                item.disabledChildFirstCategory.SumOfPrice = sum;
                            }
                            if (categoryId.Equals(childhood2CategoryId))
                            {
                                item.disabledChildSecondCategory.Count += 1;
                                item.disabledChildSecondCategory.SumOfPrice = sum;
                            }
                            if (categoryId.Equals(childhood3CategoryId))
                            {
                                item.disabledChildThirdCategory.Count += 1;
                                item.disabledChildThirdCategory.SumOfPrice = sum;
                            }
                            if (categoryId.Equals(commonDeseas1CategoryId))
                            {
                                item.disabledGeneralIlnessFirstCategory.Count += 1;
                                item.disabledGeneralIlnessFirstCategory.SumOfPrice = sum;
                            }
                            if (categoryId.Equals(commonDeseas2CategoryId))
                            {
                                item.disabledGeneralIlnessSecondCategory.Count += 1;
                                item.disabledGeneralIlnessSecondCategory.SumOfPrice = sum;
                            }
                            if (categoryId.Equals(commonDeseas3CategoryId))
                            {
                                item.disabledGeneralIlnessThirdCategory.Count += 1;
                                item.disabledGeneralIlnessThirdCategory.SumOfPrice = sum;
                            }
                            if (categoryId.Equals(retirementCategoryId))
                            {
                                item.agedCitizens.Count += 1;
                                item.agedCitizens.SumOfPrice = sum;
                            }
                            if (categoryId.Equals(alpineRetirementCategoryId))
                            {
                                item.agedHighlandResident.Count += 1;
                                item.agedHighlandResident.SumOfPrice = sum;
                            }
                            if (categoryId.Equals(heroMotherCategoryId))
                            {
                                item.motherHeroine.Count += 1;
                                item.motherHeroine.SumOfPrice = sum;
                            }
                            if (categoryId.Equals(childSurvivorsCategoryId))
                            {
                                item.childLoseBreadWinner.Count += 1;
                                item.childLoseBreadWinner.SumOfPrice = sum;
                            }
                            if (categoryId.Equals(childSurvivorsCategoryId))
                            {
                                item.childLoseBreadWinner.Count += 1;
                                item.childLoseBreadWinner.SumOfPrice = sum;
                            }
                            if (categoryId.Equals(orphanChildren))
                            {
                                item.childOrphan.Count += 1;
                                item.childOrphan.SumOfPrice = sum;
                            }
                        }
                    }
                }
                if (item.allReceiverUBK.SumOfPrice > 0)
                    item.averageSumPriceUBK = Math.Round((item.averageSumPriceUBK / item.allReceiverUBK.Count), 2);
                else item.averageSumPriceUBK = item.allReceiverUBK.Count / 1;
                item.allReceiverBalagaSuyunchu = GetBalagaSuyunchu(context, year, month);
                return item;
            }

            public static SubItem GetBalagaSuyunchu( WorkflowContext context, int year, int month)
            {
                decimal BALAGA_SUYUNCHU_PRICE = 4000;
                var subItem = new SubItem();
                var fd = new DateTime(year, month, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var qb = new QueryBuilder(balagaSuyunchuApp, context.UserId);

                qb.And("&State")
                   .In(new object[]
                   {
                      payable_state
                   })
                   .And("DocDate").Ge(fd).And("DocDate").Le(ld);

                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var applicantSrc = query.JoinSource(query.Source, applicantId, SqlSourceJoinType.Inner, "Applicant");      
                query.AddAttribute(applicantSrc, "&Id", SqlQuerySummaryFunction.Count);

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        var count = reader.IsDbNull(0) ? 0 : reader.GetInt32(0);
                        subItem.Count = count;
                    }
                }
                subItem.SumOfPrice = BALAGA_SUYUNCHU_PRICE;
             return subItem;
            }
        
    }
        public static class FC_1022
        {
            private static readonly Guid uyBulogoKomokPaymentId = new Guid("{3193A90D-380B-4428-B7B1-04C548AA902E}");
            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}");
            private static readonly Guid postOrderDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid bankOrderDefId = new Guid("{A7D9505F-F3BE-4ABC-95E7-129D907C0FD8}");
            private static readonly Guid orderPaymentDefId = new Guid("{AD83752B-C412-4FEC-A345-BB0495C34150}");
            private static readonly Guid assignmentDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");
            private static readonly Guid reportDefId = new Guid("{1A09ECD6-55E1-4307-862E-6F98F47E252C}");
            private static readonly Guid reportItemDefId = new Guid("{2A8709AB-3522-4019-A29F-5C333893645B}");
            private static readonly Guid bankDefId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");
            private static readonly Guid postDefId = new Guid("{19EA8D75-2EE7-42CA-BE3B-D7E41F343DDD}");
            private static readonly Guid section1TypeId = new Guid("{2A273790-9091-4DBD-A712-12D46578196C}");
            private static readonly Guid section2TypeId = new Guid("{0B6C58B1-A6F1-4455-8092-9B8583ADA295}");
            private static readonly Guid tariffDefId = new Guid("{0F29B75F-DE90-4910-9524-B74CB0418A57}");

            private static readonly Guid categoryType1Id = new Guid("{12C16D1F-1AF5-4BB0-931A-2D1D9F07E1A5}");//Детям погибшего в событиях 17-18 марта 2002 года  в Аксыйском районе Джалал-Абадской области
            private static readonly Guid categoryType2Id = new Guid("{1BFDE4A6-D55F-435A-A34E-B77FAAEA265B}");//Родителям погибшего в событиях 17-18 марта 2002 года  в Аксыйском районе Джалал-Абадской области
            private static readonly Guid categoryType3Id = new Guid("{4F53C318-8590-453C-9A06-B7CD9DB29888}");//Вдовам погибшего в событиях 17-18 марта 2002 года  в Аксыйском районе Джалал-Абадской области
            private static readonly Guid categoryType4Id = new Guid("{E049D13F-DE27-4644-8473-E1F50F5C5611}");//Лицам, признанным инвалидами  в событиях 17-18 марта 2002 года  в Аксыйском районе Джалал-Абадской области

            private static readonly Guid familyMemberDefId = new Guid("{85B03F9E-47D7-4829-8041-0CDCB8486572}");
            private static readonly Guid membershipTypeId = new Guid("{586102C3-962A-49CB-A914-87F2139986A3}");
            private static readonly Guid applicantId = new Guid("{6F5B8A06-361E-4559-8A53-9CB480A9B16C}");

            private static readonly Guid retirementBenefitPaymentId = new Guid("{AFAA7F86-74AE-4260-9933-A56F7845E55A}");
            private static readonly Guid childBenefitPaymentId = new Guid("{AB3F8C41-897A-4574-BAA0-B7CD4AAA1C80}");
            private static readonly Guid invalidBenefitPaymentId = new Guid("{70C28E62-2387-4A59-917D-A366ADE119A8}");
            private static readonly Guid survivorBenefitPaymentId = new Guid("{839D5712-E75B-4E71-83F7-168CE4F089C0}");
            private static readonly Guid aidsFromBenefitPaymentId = new Guid("{3BEBE4F9-0B15-41CB-9B96-54E83819AB0F}");
            private static readonly Guid aidsBenefitPaymentId = new Guid("{47EEBFBC-A4E9-495D-A6A1-F87B5C3057C9}");
            private static readonly Guid orphanBenefitPaymentId = new Guid("{4F12C366-7E2F-4208-9CB8-4EAB6E6C0EF1}");


            private static readonly List<Guid> SocialBenefitsList = new List<Guid>
            {
                retirementBenefitPaymentId,childBenefitPaymentId,invalidBenefitPaymentId,
                survivorBenefitPaymentId,aidsFromBenefitPaymentId,aidsBenefitPaymentId,
                orphanBenefitPaymentId
            };
            private static readonly Guid despAksyPaymentTypeId = new Guid("{E590688C-FE0E-4DE2-BEFC-35887CD23ABA}");

            private static readonly List<Guid> AksyCategoryList = new List<Guid>
            {
                categoryType1Id, categoryType2Id,categoryType3Id,categoryType4Id
            };
            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {
                var items = new List<ReportItem>();
                var fd = new DateTime(year, month, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var qb = new QueryBuilder(postDefId, context.UserId);
                qb.Where("Application")
                   .Include("PaymentCategory")
                   .In(new object[]
                    {
                    categoryType1Id,
                    categoryType2Id,
                    categoryType3Id,
                    categoryType4Id
                    }).End().And
                   ("ExpiryDate").Ge(fd);
                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var applicationSrc = query.JoinSource(query.Source, appDefId, SqlSourceJoinType.Inner, "Application");
                var applicantSrc = query.JoinSource(applicationSrc, applicantId, SqlSourceJoinType.Inner, "Applicant");

                var assignmentSrc = query.JoinSource(applicationSrc, assignmentDefId, SqlSourceJoinType.Inner, "Assignments");
                var personSrc = query.JoinSource(assignmentSrc, applicantId, SqlSourceJoinType.Inner, "Person");
 


                query.AddAttribute(applicationSrc, "Town");
                query.AddAttribute(applicationSrc, "Street");
                query.AddAttribute(applicationSrc, "House");
                query.AddAttribute(applicationSrc, "Apartment");

                query.AddAttribute(applicantSrc, "FirstName");
                query.AddAttribute(applicantSrc, "MiddleName");
                query.AddAttribute(applicantSrc, "LastName");
                query.AddAttribute(applicantSrc, "BirthDate");

                query.AddAttribute(personSrc, "FirstName");
                query.AddAttribute(personSrc, "MiddleName");
                query.AddAttribute(personSrc, "LastName");
                query.AddAttribute(personSrc, "BirthDate");
                query.AddAttribute(assignmentSrc, "MembershipType");
                query.AddAttribute(assignmentSrc, "Amount");
                query.AddAttribute(applicationSrc, "&Id");
                query.AddAttribute(applicationSrc, "PaymentSum");
                query.AddAttribute(applicationSrc, "PaymentCategory");
                query.AddAttribute(applicationSrc, "PaymentType");


                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {  
                        var town = reader.IsDbNull(0) ? "-" : reader.GetString(0);
                        var street = reader.IsDbNull(1) ? "-" : reader.GetString(1);
                        var house = reader.IsDbNull(2) ? "-" : reader.GetString(2);
                        var apartment = reader.IsDbNull(3) ? "-" : reader.GetString(3);

                        var firstNameApplicant = reader.IsDbNull(4) ? "-" : reader.GetString(4);
                        var middleNameApplicant = reader.IsDbNull(5) ? "-" : reader.GetString(5);
                        var lastNameApplicant = reader.IsDbNull(6) ? "-" : reader.GetString(6);
                        var birthDateApplicant = reader.IsDbNull(7) ? DateTime.MinValue : reader.GetDateTime(7);

                        var firstNameMember = reader.IsDbNull(8) ? "-" : reader.GetString(8);
                        var middleNameMember = reader.IsDbNull(9) ? "-" : reader.GetString(9);
                        var lastNameMember = reader.IsDbNull(10) ? "-" : reader.GetString(10);
                        var birthDateMember = reader.IsDbNull(11) ? DateTime.MinValue : reader.GetDateTime(11);

                        var membershipTypeId = reader.IsDbNull(12) ? Guid.Empty : reader.GetGuid(12);
                        var amountMember = reader.IsDbNull(13) ? 0m : reader.GetDecimal(13);
                        var applicantId = reader.IsDbNull(14) ? Guid.Empty : reader.GetGuid(14);
                        var paymentSumApplicant = reader.IsDbNull(15) ? 0m : reader.GetDecimal(15);
                        var categoryId= reader.IsDbNull(16) ? Guid.Empty : reader.GetGuid(16);
                        var paymentTypeId = reader.IsDbNull(17) ? Guid.Empty : reader.GetGuid(17);

                        if (AksyCategoryList.Contains(categoryId))
                        {

                            var item = GetReportItem(items, applicantId);
                            item.applicant.address = town + " " + street + " " + house + " " + apartment;
                            item.applicant.fullName = firstNameApplicant + " " + middleNameApplicant + " " + lastNameApplicant;
                           
                            item.applicant.membershipType = "";
                            item.applicant.yearOfBirth = birthDateApplicant;

                            if  (despAksyPaymentTypeId.Equals(paymentTypeId))
                            {
                                item.applicant.priceAddSocialBenefit = paymentSumApplicant;
                                item.applicant.sumFromBeginOfYearAddSocialBenefit = paymentSumApplicant * month;
                            }
                            if (AksyCategoryList.Contains(paymentTypeId))
                            {
                                item.applicant.priceSocialBenefit = paymentSumApplicant;
                                item.applicant.sumFromBeginOfYear = paymentSumApplicant * month;
                            }
                            item.applicant.totalSumFromBeginOfYear = item.applicant.sumFromBeginOfYearAddSocialBenefit + item.applicant.sumFromBeginOfYear;
                            item.applicant.categoryName = context.Enums.GetValue(categoryId).Value;
                            
                            if (! membershipTypeId.Equals(Guid.Empty))
                            {
                                var memberOfFamily = new SubReportItem();
                                memberOfFamily.fullName = firstNameMember + " " + middleNameMember + " " + lastNameMember;
                                memberOfFamily.membershipType = context.Enums.GetValue(membershipTypeId).Value;
                                memberOfFamily.yearOfBirth = birthDateMember;

                                if (despAksyPaymentTypeId.Equals(paymentTypeId))
                                {
                                    memberOfFamily.priceAddSocialBenefit = amountMember;
                                    memberOfFamily.sumFromBeginOfYearAddSocialBenefit = amountMember * month;
                                }
                                if (AksyCategoryList.Contains(paymentTypeId))
                                {
                                    memberOfFamily.priceSocialBenefit = amountMember;
                                    memberOfFamily.sumFromBeginOfYear = amountMember * month;
                                }
                                memberOfFamily.totalSumFromBeginOfYear = memberOfFamily.sumFromBeginOfYearAddSocialBenefit + memberOfFamily.sumFromBeginOfYear;
                                memberOfFamily.categoryName = context.Enums.GetValue(categoryId).Value;
                                item.membersOfFamily.Add(memberOfFamily);
                            }                            
                        }                      
                    }
                }
                foreach (var item in items )
                {
                    item.totalSBsumFromBeginOfYear = item.applicant.sumFromBeginOfYear;
                    item.totalAddSBsumFromBeginOfYear = item.applicant.sumFromBeginOfYearAddSocialBenefit;
                    item.totalSumFromBeginOfYear = item.applicant.totalSumFromBeginOfYear;

                    foreach (var subItem in item.membersOfFamily)
                    {
                        item.totalSBsumFromBeginOfYear = item.totalSBsumFromBeginOfYear + subItem.sumFromBeginOfYear;
                        item.totalAddSBsumFromBeginOfYear = item.totalAddSBsumFromBeginOfYear + subItem.sumFromBeginOfYearAddSocialBenefit;
                        item.totalSumFromBeginOfYear = item.totalSumFromBeginOfYear + subItem.totalSumFromBeginOfYear;
                    }
                }
                return items;
            }

            static ReportItem GetReportItem(List<ReportItem> items, Guid _applicantId)
            {
                var item = items.FirstOrDefault(x => x.getApplicantId().Equals(_applicantId));
                if (item != null) return item;
                item = new ReportItem(_applicantId);
                items.Add(item);
                return item;
            }

            [DataContract]
            public class ReportItem
            {
                [DataMember]
                private Guid applicantId { get; set; }
                [DataMember]
                public SubReportItem applicant { get; set; }
                [DataMember]
                public List<SubReportItem> membersOfFamily { get; set; }
                
                public decimal totalSBsumFromBeginOfYear { get; set; }
                public decimal totalAddSBsumFromBeginOfYear { get; set; }
                public decimal totalSumFromBeginOfYear { get; set; }

                public Guid getApplicantId()
                {
                    return applicantId;
                }

                public ReportItem(Guid ApplicantId)
                {
                    applicantId = ApplicantId;
                    applicant = new SubReportItem();
                    membersOfFamily = new List<SubReportItem>();
                }
            }

            [DataContract]
            public class SubReportItem
            {
                [DataMember]
                public string fullName { get; set; }
                [DataMember]
                public string address { get; set; }
                [DataMember]
                public string membershipType { get; set; }
                [DataMember]
                public DateTime yearOfBirth { get; set; }
                [DataMember]
                public decimal priceSocialBenefit { get; set; }
                [DataMember]
                public decimal sumFromBeginOfYear { get; set; }
                [DataMember]
                public decimal priceAddSocialBenefit { get; set; }
                [DataMember]
                public decimal sumFromBeginOfYearAddSocialBenefit { get; set; }
                [DataMember]
                public decimal totalSumFromBeginOfYear { get; set; }
                [DataMember]
                public string categoryName { get; set; }

            }

        }

        public static class Report_1032A
        {
            private static readonly Guid appDefId = new Guid("{04D25808-6DE9-42F5-8855-6F68A94A224C}"); //Заявления
            private static readonly Guid assigneDefId = new Guid("{5D599CE4-76C5-4894-91CC-4EB3560196CE}");  //Назначение
            private static readonly Guid personDefId = new Guid("{6F5B8A06-361E-4559-8A53-9CB480A9B16C}"); //person
            private static readonly Guid ubkEnumId = new Guid("{3193A90D-380B-4428-B7B1-04C548AA902E}"); // 

            private static readonly Guid approvedStateId = new Guid("{3DB4DB00-3A7F-4228-A9A3-A413B85C18B4}");  //На выплате

            public static List<Guid> bishkekList = new List<Guid>//г.Бишкек
            {                                
            //new Guid("{E59E153E-4FE9-4872-BBD0-9E183793EFEF}"),
                new Guid("{2AB93962-2A1F-42D4-9E70-78931B9A413D}"),//ленин
                new Guid("{34DDCAF2-EB08-48E7-894A-29C929D62C83}"),//первомайский
                new Guid("{0BADBCA1-ADD3-4B74-9A95-60F2EED92118}"),//свердлово
                new Guid("{17C0AC69-2247-41E8-B086-54599FE11CED}"),//октябрь
            };
            public static List<Guid> chuiList = new List<Guid>//Чуйская область
            {                                                    
            //    new Guid("{41E34648-6C9D-44D0-8CA5-941DB051B163}"),
                new Guid("{78D008DF-F7E8-4F99-92A1-42E7AE6E34C3}"),//кемин
                new Guid("{AA466C30-47A6-4252-989D-F44C91A120A5}"),//чуй
                new Guid("{CAE58A37-FBE8-49AA-8E09-B167F871A5E8}"),//город токмок
                new Guid("{BEC45992-2D1C-4488-A260-FC7C5511F619}"),//ыссык-ата
                new Guid("{822E0311-F790-474D-BC97-E67CE8D14009}"),//аламудун
                new Guid("{C139D4DE-9F64-46D2-A908-1C477B78ECCD}"),//сокулук
                new Guid("{CA59DB1A-4AB2-4CB2-90C6-BF323E82E44A}"),//москва
                new Guid("{6612E6F7-BA2B-4E33-ADB9-FA0B14705E27}"),//жайыл
                new Guid("{72075FFB-371D-4C7B-8A5A-993E1C51CBFF}"),//панфилов
            };
            public static List<Guid> talasList = new List<Guid>//Таласская область
            {                                   
            //    new Guid("{BC710978-8C0B-4063-8BA9-A12F75DE4829}"),
                new Guid("{02992EB8-E107-408E-954E-E322A73B433A}"),//г талас
                new Guid("{1E5868C3-B522-4DBF-BC8F-02C75A899C0F}"),//талас рн
                new Guid("{C285A1F2-2114-4752-A626-A235C6D6F2B1}"),//бакай-ата
                new Guid("{17A6CA38-8B7B-4F5C-973B-18F31646CC03}"),//кара-буура
                new Guid("{376FF578-372D-43A4-818E-31BC64327BEF}"),//манас
            };
            public static List<Guid> issyk_kulList = new List<Guid>//Иссык-Кульская область
            {
            //    new Guid("{AD009808-FBAC-43CA-8671-5E790C213497}"),
                new Guid("{43842F64-7BB7-45EC-930B-54AD19186382}"),//каракол
                new Guid("{2FEA2DF5-24F6-4E53-BF6C-22604AC014C1}"),//балыкчы
                new Guid("{416366EE-580B-4E57-8641-0D90A2F5AB73}"),//иссык-куль
                new Guid("{5B438995-2070-46A8-9377-E922F0D64E4F}"),//тюп
                new Guid("{AAD91EAB-297F-40C8-B592-68573495EEC0}"),//жети-огуз
                new Guid("{55F9FFBF-B789-4002-BD64-B04163806897}"),//тон
                new Guid("{D2EDB83E-7EE5-4B5A-82A6-E9E49DD1E3B3}"),//ак-суу
            };
            public static List<Guid> narynList = new List<Guid>//Нарынская область
            {
            //    new Guid("{CF4CA271-9202-4A5A-AE49-EB6237C33982}"),
                new Guid("{CB788BA9-8F94-4317-A655-B4F5527F3A4B}"),//акталаа
                new Guid("{4C60A2A2-73B4-4EA4-A6D0-E2B41370C7FC}"),//ат-баши
                new Guid("{52568E10-0210-4B09-B106-9E14520E26F1}"),//жумгал
                new Guid("{B631F3B0-1656-49C5-9152-0252F304D29B}"),//кочкор
                new Guid("{A6B9B397-81CF-43ED-A17B-6A13C110A678}"),//нарын район
                new Guid("{0A409F28-4A73-4CED-B368-10C6BE53419F}"),//нарын г
            };
            public static List<Guid> batkenList = new List<Guid>//Баткенская область
            {
            //    new Guid("{4E98AD82-8ED4-4727-8E24-678921AB534C}"),
                new Guid("{0BCED873-4950-449A-98E6-69C44F64D70C}"),//баткен рн
                new Guid("{20745158-EAE3-434C-BDF5-5F893C8963ED}"),//баткен г
                new Guid("{E062B2A3-42C4-4619-B086-705E6C5367D3}"),//кадамжай
                new Guid("{F6CECF6A-2D5D-44C1-BC2F-29A836965531}"),//лейлек
                new Guid("{DDE31E57-4454-44EE-AFA9-80227E3C8620}"),//кызыл-кия
                new Guid("{E883C8F3-5E3C-4086-A078-91FD22E6550A}"),//сулюкта
            };
            public static List<Guid> oshList = new List<Guid>//Ошская область
            {
            //    new Guid("{E01E9890-2A41-454E-9EC1-3D4AAF50ED62}"),
                new Guid("{C3C63817-FFB3-4905-BC70-8C3A3DBB67DC}"),//араван
                new Guid("{2A0481C0-FB12-4048-9153-CB7AE997A26C}"),//каракулджа
                new Guid("{3A305A9A-D30E-4C38-9E41-09DF993A658E}"),//кара-суу
                new Guid("{E0AF1DF7-AA64-45DD-873E-510CD413AD35}"),//ноокат
                new Guid("{B0B7490F-E48D-4DC7-9548-0505D73C858F}"),//узген рн
                new Guid("{2F8C280E-6C97-41D5-B24F-7D0DF4FF4C0E}"),//узген г
                new Guid("{5903E184-79E4-4630-AD91-FD488B84B832}"),//алай
                new Guid("{8A7D0C1C-A4A7-4582-8D65-8023CDB273E1}")//чон-алай
            };
            public static List<Guid> osh = new List<Guid>
            {
                new Guid ("{A99E469A-E8D4-4139-B89A-CE4AF6AA0733}")
            };
            public static List<Guid> jalal_abad = new List<Guid>//Джалал-Абадская область
            {
            //    new Guid("{3ED50EFB-3E20-407C-93CB-9D8E0EF15B1B}"),
                new Guid("{D319DC07-E7F7-4997-AFCD-17CEAD707B7F}"),//г джалалабад
                new Guid("{8CAC9C70-D770-4AAB-8B0B-E1C432F6985A}"),//кок-жангак
                new Guid("{EFA2A70A-D10E-4941-B1F8-AE5218E5AD29}"),//майлысуу
                new Guid("{8E3CA117-0956-4F67-A8EF-B8222DC0A21E}"),//ташкумыр
                new Guid("{CB86E66E-8A41-4D92-9FD6-9BCF7E61F543}"),//каракуль
                new Guid("{9375DF49-1B41-4EFD-BB79-0FC2A86ABED6}"),//сузак
                new Guid("{A23770C6-2843-446E-8382-8C50AFD699E5}"),//базар-коргон
                new Guid("{A3E2A017-96F2-47C6-AF09-6FA4287E6953}"),//ноокен
                new Guid("{A5B70AEE-4E8B-4194-864D-F58E5BAB76E7}"),//аксы
                new Guid("{915D0BEB-53C1-440F-9CBA-00612369FEFC}"),//ала-бука
                new Guid("{5026031D-2BC9-4A24-8DB5-30C7DD992352}"),//чаткал
                new Guid("{F9D53DFB-0E18-44E6-A905-8B67F5751D37}"),//токтогул рн
                new Guid("{BF0A92AD-140E-4582-AB9F-DB304890C35A}")//тогуз-торо
            };
            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {
                var fd = new DateTime(year, month, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                return CalcItems(context, fd, ld);
            }
            public static List<ReportItem> CalcItems(WorkflowContext context, DateTime fd, DateTime ld)
            {
                var items = new List<ReportItem>();
                var qb = new QueryBuilder(appDefId, context.UserId);
                qb.Where("&State").Eq(approvedStateId).And("PaymentType").Eq(ubkEnumId).And("LastDocumentDate").Ge(fd).And("LastDocumentDate").Le(ld);

                var query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var assignSrc = query.JoinSource(query.Source, assigneDefId, SqlSourceJoinType.Inner, "Assignments");

                query.AddAttribute(query.Source, "&Id");
                query.AddAttribute(assignSrc, "EmploymentStatus");
                query.AddAttribute(assignSrc, "MembershipType");
                query.AddAttribute(query.Source, "&OrgId");
                query.AddAttribute(assignSrc, "Amount");

                var table = new DataTable();
                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    reader.Open();
                    reader.Fill(table);
                    reader.Close();
                }
                foreach (DataRow row in table.Rows)
                {
                    var appId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                    var empStatus = row[1] is DBNull ? Guid.Empty : (Guid)row[1];
                    var memberType = row[2] is DBNull ? Guid.Empty : (Guid)row[2];
                    var orgId = row[3] is DBNull ? Guid.Empty : (Guid)row[3];
                    var amount = row[4] is DBNull ? 0m : (decimal)row[4];

                    var orgNameRayon = context.Orgs.GetOrgName(orgId);
                    var rayon = GetReportItem(items, orgNameRayon, orgId);

                    if (appId != Guid.Empty)
                    {
                        rayon.gr1 += 1;
                        //Ребенок до 3-х лет + Ребенок от 3-х до 16 лет
                        if ((empStatus.Equals(new Guid("{D8FF3DAF-A701-414A-B965-4BF93BB658B9}"))) || (empStatus.Equals(new Guid("{8024982A-4AFB-4074-9D63-3EEFE22420E0}"))))
                            rayon.gr2 += 1;
                        if (memberType.Equals(new Guid("{9BF3B519-C0B8-4344-BF29-47B062C07454}"))) //Опекаемый
                            rayon.gr3 += 1;
                        if (memberType.Equals(new Guid("{45D55628-5E72-42B8-8B8D-667346E79046}"))) //Двойня
                            rayon.gr4 += 1;
                        if (memberType.Equals(new Guid("{8CCAE21E-128A-4728-9479-9C094271C614}"))) //Тройня и более
                            rayon.gr5 += 1;

                        rayon.gr6 = rayon.gr2 + rayon.gr3 + rayon.gr4 + rayon.gr5;
                        if (amount != 0)
                        {
                            rayon.gr7 += Math.Round(amount);
                            rayon.gr8 = Math.Round(rayon.gr7) / rayon.gr1;
                        }
                    }
                }
               // RegionReportItem regionReportItem = new RegionReportItem();
                foreach (var subItem in items)
                {
                    if (bishkekList.Contains(subItem.GetOrgId())) subItem.RegionName = "г.Бишкек";
                        //regionReportItem.BishkekCityRegion.Add(subItem);
                    if (chuiList.Contains(subItem.GetOrgId())) subItem.RegionName = "Чуйская область";
                    //regionReportItem.ChuyRegion.Add(subItem);
                    if (talasList.Contains(subItem.GetOrgId())) subItem.RegionName = "Таласская область";
                    //regionReportItem.TalasRegion.Add(subItem);
                    if (issyk_kulList.Contains(subItem.GetOrgId())) subItem.RegionName = "Иссык-Кульская область";
                    //regionReportItem.YssykKolRegion.Add(subItem);
                    if (narynList.Contains(subItem.GetOrgId())) subItem.RegionName = "Нарынская область";
                    //regionReportItem.NarynRegion.Add(subItem);
                    if (batkenList.Contains(subItem.GetOrgId())) subItem.RegionName = "Баткенская область";
                    //regionReportItem.BatkenRegion.Add(subItem);
                    if (oshList.Contains(subItem.GetOrgId())) subItem.RegionName = "Ошская область";
                    //regionReportItem.OshRegion.Add(subItem);
                    if (osh.Contains(subItem.GetOrgId())) subItem.RegionName = "г.Ош";
                    //regionReportItem.OshCityRegion.Add(subItem);
                    if (jalal_abad.Contains(subItem.GetOrgId())) subItem.RegionName = "Джалал-Абадская область";
                    //regionReportItem.JalalAbadRegion.Add(subItem);
                }
                    return items;

            }

            [DataContract]
            public class RegionReportItem
            {
                public RegionReportItem()
                {
                    ChuyRegion = new List <ReportItem>();
                    YssykKolRegion = new List<ReportItem>();
                    NarynRegion = new List<ReportItem>();
                    TalasRegion = new List<ReportItem>();
                    OshRegion = new List<ReportItem>();
                    JalalAbadRegion = new List<ReportItem>();
                    BatkenRegion = new List<ReportItem>();
                    BishkekCityRegion = new List<ReportItem>();
                    OshCityRegion = new List<ReportItem>();
                }
                [DataMember]
                public List<ReportItem> ChuyRegion { get; set; }
                [DataMember]
                public List<ReportItem> YssykKolRegion { get; set; }
                [DataMember]
                public List<ReportItem> NarynRegion { get; set; }
                [DataMember]
                public List<ReportItem> TalasRegion { get; set; }
                [DataMember]
                public List<ReportItem> OshRegion { get; set; }
                [DataMember]
                public List<ReportItem> JalalAbadRegion { get; set; }
                [DataMember]
                public List<ReportItem> BatkenRegion { get; set; }
                [DataMember]
                public List<ReportItem> BishkekCityRegion { get; set; }
                [DataMember]
                public List<ReportItem> OshCityRegion { get; set; }
            }
            static ReportItem GetReportItem(List<ReportItem> items, string orgName, Guid orgId)
            {
                var item = items.FirstOrDefault(x => x.OrgName == orgName);
                if (item != null) return item;
                item = new ReportItem(orgId)
                {
                    OrgName = orgName,                   
                };
                items.Add(item);
                return item;
            }

            [DataContract]
            public class ReportItem
            {
                [DataMember]
                public string RegionName { get; set; }
                [DataMember]
                public string OrgName { get; set; }
                [DataMember]
                private Guid OrgId { get; set; }
                [DataMember]
                public int gr1 { get; set; }
                [DataMember]
                public int gr2 { get; set; }
                [DataMember]
                public int gr3 { get; set; }
                [DataMember]
                public int gr4 { get; set; }
                [DataMember]
                public int gr5 { get; set; }
                [DataMember]
                public int gr6 { get; set; }
                [DataMember]
                public decimal gr7 { get; set; }
                [DataMember]
                public decimal gr8 { get; set; }

                public ReportItem(Guid orgId)
                {
                    OrgId = orgId;
                }
                public Guid GetOrgId()
                {
                    return OrgId;
                }
            }
        }

        public static class Report_2006 //active code
        {
            static Guid reportNo1DefId = new Guid("{1A09ECD6-55E1-4307-862E-6F98F47E252C}");//Уй-булого комок (от 0 до 16 лет)
            static Guid reportNo1ItemDefId = new Guid("{2A8709AB-3522-4019-A29F-5C333893645B}");//Rows Уй-булого комок (от 0 до 16 лет)

            static Guid reportNo2DefId = new Guid("{0C6A34A9-CF41-4750-B21D-9913672A0C76}");//ЕПМС
            static Guid reportNo2ItemDefId = new Guid("{760233C7-3FC3-4417-95AE-F399B8B0208F}");//Rows ЕПМС

            static Guid reportNo3DefId = new Guid("{9118D82A-2AB4-40F2-A3BC-0BB54D34F3CE}");//Социальное пособие  (ЕСП)
            static Guid reportNo3ItemDefId = new Guid("{54167C45-6382-460E-8D72-CDE7D7B43F5C}");//Rows Социальное пособие  (ЕСП)

            static Guid reportNo4DefId = new Guid("{9580E9AE-5949-4B83-90F0-EED511B63477}");//ДЕСП
            static Guid reportNo4ItemDefId = new Guid("{D2FCA75E-34E4-4E85-93B5-6917C4F18BC2}");//Rows ДЕСП

            private static readonly Guid BenefitPaymentId0 = new Guid("{330FA388-7596-4D4B-903B-33D4D069707D}");   // Уй-булого комок от 0 до 16 лет
            private static readonly Guid BenefitPaymentId1 = new Guid("{7F1B9709-8F99-473F-9AE0-2DDCD74BDE6E}");   // * Пособие матерям родившим двойню до достижения 3-лет
            private static readonly Guid BenefitPaymentId2 = new Guid("{9BC8A898-31F8-4F55-8C14-28F641142370}");   // * ЕПМС на ребенка до 3-х лет
            private static readonly Guid BenefitPaymentId3 = new Guid("{64ACC17D-78B8-492E-AC81-7B1E4750F53A}");   // * ЕПМС матерям родившим тройню и более до достижения 16-лет
            private static readonly Guid BenefitPaymentId4 = new Guid("{D24151CF-C8B0-4851-B0EC-6D6EB382DC61}"); //* ЕПМС семьям по малообеспеченности
            private static readonly Guid BenefitPaymentId5 = new Guid("{BCE5B287-7495-4AD1-96A8-F52040A4CABF}"); //*ЕПМС малоимущим (опекаемым, сиротам) 
            private static readonly Guid approvedStateId = new Guid("{66D7FA1C-77EF-470D-A70B-0D6E5E16D942}"); // Утвержден
            private static readonly Guid oktybrOrgId = new Guid("{17C0AC69-2247-41E8-B086-54599FE11CED}"); //Октябрьский район для ЕПМС

            public static List<GeneralReportItem> Execute(WorkflowContext context, int year, int month)
            {
                List<GeneralReportItem> reportItems = new List<GeneralReportItem>();
                for (int i = 1; i < 4; i++)
                {
                    QueryBuilder qb = new QueryBuilder(reportNo1DefId, context.UserId);
                    SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                    SqlQuerySource itemSrc = query.Source;

                    switch (i)
                    {
                        case 1:
                            {
                                qb = new QueryBuilder(reportNo1DefId, context.UserId);
                                qb.Where("&State").Eq(approvedStateId).And("Year").Eq(year).And("Month").Eq(month);
                                query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                                itemSrc = query.JoinSource(query.Source, reportNo1ItemDefId, SqlSourceJoinType.Inner, "Rows");
                                query.AddCondition(ExpressionOperation.And, reportNo1ItemDefId, "PaymentType", ConditionOperation.Equal, BenefitPaymentId0);
                                break;
                            }
                        case 2:
                            {
                                qb = new QueryBuilder(reportNo2DefId, context.UserId);
                                qb.Where("&State").Eq(approvedStateId).And("Year").Eq(year).And("Month").Eq(month).And("&OrgId").Eq(oktybrOrgId);
                                query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                                itemSrc = query.JoinSource(query.Source, reportNo2ItemDefId, SqlSourceJoinType.Inner, "Rows");
                                query.AddCondition(ExpressionOperation.And, reportNo2ItemDefId, "PaymentType", ConditionOperation.In, new object[] { BenefitPaymentId4, BenefitPaymentId3 });
                                break;
                            }
                        case 3:
                            {
                                qb = new QueryBuilder(reportNo3DefId, context.UserId);
                                qb.Where("&State").Eq(approvedStateId).And("Year").Eq(year).And("Month").Eq(month);
                                query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                                itemSrc = query.JoinSource(query.Source, reportNo3ItemDefId, SqlSourceJoinType.Inner, "Rows");                                
                                break;
                            }
                    }

                    query.AddAttribute("&OrgId");
                    query.AddAttribute(itemSrc, "PostSection2AppCount", SqlQuerySummaryFunction.Sum);
                    query.AddAttribute(itemSrc, "PostSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                    query.AddAttribute(itemSrc, "PostSection2DocCount", SqlQuerySummaryFunction.Sum);

                    query.AddAttribute(itemSrc, "BankSection2AppCount", SqlQuerySummaryFunction.Sum);
                    query.AddAttribute(itemSrc, "BankSection2NeedAmount", SqlQuerySummaryFunction.Sum);
                    query.AddAttribute(itemSrc, "BankSection2DocCount", SqlQuerySummaryFunction.Sum);
                    query.AddGroupAttribute("&OrgId");

                    var table = new DataTable();
                    using (var reader = new SqlQueryReader(context.DataContext, query))
                    {
                        reader.Open();
                        reader.Fill(table);
                        reader.Close();
                    }
                    foreach (DataRow row in table.Rows)
                    {
                        var orgId = row[0] is DBNull ? Guid.Empty : (Guid)row[0];
                        var postSection2AppCount = row[1] is DBNull ? 0 : (int)row[1];
                        var postSection2NeedAmount = row[2] is DBNull ? 0m : (decimal)row[2];
                        var postSection2DocCount = row[3] is DBNull ? 0 : (int)row[3];

                        var bankSection2AppCount = row[4] is DBNull ? 0 : (int)row[4];
                        var bankSection2NeedAmount = row[5] is DBNull ? 0m : (decimal)row[5];
                        var bankSection2DocCount = row[6] is DBNull ? 0 : (int)row[6];

                        var orgName = context.Orgs.GetOrgName(orgId);
                        var item = GetReportItem(reportItems, orgName, orgId);
                        switch (i)
                        {
                            case 1:
                                {
                                    item.ubkPayment.Section2AppCount = postSection2AppCount+ bankSection2AppCount;
                                    item.ubkPayment.AverageNeedAmount = postSection2NeedAmount + bankSection2NeedAmount;
                                    item.ubkPayment.Section2DocCount = postSection2DocCount+ bankSection2DocCount;
                                    item.NeedAmountFromBeginOfYear += item.ubkPayment.AverageNeedAmount;
                                    break;
                                }
                            case 2:
                                {
                                    item.lifMonthlyGrant.Section2AppCount = postSection2AppCount + bankSection2AppCount;
                                    item.lifMonthlyGrant.AverageNeedAmount = postSection2NeedAmount + bankSection2NeedAmount;
                                    item.lifMonthlyGrant.Section2DocCount = postSection2DocCount + bankSection2DocCount;
                                    item.NeedAmountFromBeginOfYear += item.lifMonthlyGrant.AverageNeedAmount;
                                    break;
                                }
                            case 3:
                                {
                                    item.monthlyGrant.Section2AppCount = postSection2AppCount + bankSection2AppCount;
                                    item.monthlyGrant.AverageNeedAmount = postSection2NeedAmount + bankSection2NeedAmount;
                                    item.monthlyGrant.Section2DocCount = postSection2DocCount + bankSection2DocCount;
                                    item.NeedAmountFromBeginOfYear += item.monthlyGrant.AverageNeedAmount;
                                    break;
                                }
                        }
                    }
                }

                foreach (var item in reportItems)
                {
                    if (item.ubkPayment.Section2AppCount > 0) item.ubkPayment.AverageNeedAmount = Math.Round(item.ubkPayment.AverageNeedAmount / item.ubkPayment.Section2AppCount, 1);
                    if (item.lifMonthlyGrant.Section2AppCount > 0) item.lifMonthlyGrant.AverageNeedAmount = Math.Round(item.lifMonthlyGrant.AverageNeedAmount / item.lifMonthlyGrant.Section2AppCount, 1);
                    if (item.monthlyGrant.Section2AppCount > 0) item.monthlyGrant.AverageNeedAmount = Math.Round(item.monthlyGrant.AverageNeedAmount / item.monthlyGrant.Section2AppCount, 1);

                    if (bishkekList.Contains(item.getOrgId())) item.ParentRegion = "г.Бишкек";
                    if (chuiList.Contains(item.getOrgId())) item.ParentRegion = "Чуйская область";
                    if (talasList.Contains(item.getOrgId())) item.ParentRegion = "Таласская область";
                    if (issyk_kulList.Contains(item.getOrgId())) item.ParentRegion = "Иссык-Кульская область";
                    if (narynList.Contains(item.getOrgId())) item.ParentRegion = "Нарынская область";
                    if (batkenList.Contains(item.getOrgId())) item.ParentRegion = "Баткенская область";
                    if (oshList.Contains(item.getOrgId())) item.ParentRegion = "Ошская область";
                    if (osh.Contains(item.getOrgId())) item.ParentRegion = "г.Ош";
                    if (jalal_abad.Contains(item.getOrgId())) item.ParentRegion = "Джалал-Абадская область";
                }

                return reportItems;
            }

        static GeneralReportItem GetReportItem(List<GeneralReportItem> items, string orgName, Guid orgId)
        {
         var item = items.FirstOrDefault(x => x.OrgName == orgName);
         if (item != null) return item;
             item = new GeneralReportItem(orgId)
              {
                OrgName = orgName,
              };
         items.Add(item);
         return item;
        }

            [DataContract]
            public class GeneralReportItem
            {
                [DataMember]
                private Guid OrgId { get; set; }
                [DataMember]
                public string ParentRegion { get; set; }
                [DataMember]
                public string OrgName { get; set; }
                [DataMember]
                public ReportItem ubkPayment { get; set; }
                [DataMember]
                public ReportItem lifMonthlyGrant { get; set; } //ЕПМС
                [DataMember]
                public ReportItem monthlyGrant { get; set; }//ЕПС
                [DataMember]
                public decimal NeedAmountFromBeginOfYear { get; set; }// пока сделаем сумму 3х выплат до выяснения
                public GeneralReportItem(Guid orgId)
                {
                    OrgId = orgId;
                    ubkPayment = new ReportItem("Уй булого комок");
                    lifMonthlyGrant = new ReportItem("ЕПМС");
                    monthlyGrant = new ReportItem("ЕСП");
                }
                public Guid getOrgId()
                {
                    return OrgId;
                }
            }

            [DataContract]
            public class ReportItem
            {
                public ReportItem(string paymentName)
                {
                    PaymentName = paymentName;
                }
                [DataMember]
                public string PaymentName { get; set; }
                [DataMember]
                public int Section2DocCount { get; set; }
                [DataMember]
                public int Section2AppCount { get; set; }
                [DataMember]
                public decimal AverageNeedAmount { get; set; }

            }

            public static List<Guid> bishkekList = new List<Guid>//г.Бишкек
            {                                
            //new Guid("{E59E153E-4FE9-4872-BBD0-9E183793EFEF}"),
                new Guid("{2AB93962-2A1F-42D4-9E70-78931B9A413D}"),//ленин
                new Guid("{34DDCAF2-EB08-48E7-894A-29C929D62C83}"),//первомайский
                new Guid("{0BADBCA1-ADD3-4B74-9A95-60F2EED92118}"),//свердлово
                new Guid("{17C0AC69-2247-41E8-B086-54599FE11CED}"),//октябрь
            };
            public static List<Guid> chuiList = new List<Guid>//Чуйская область
            {                                                    
            //    new Guid("{41E34648-6C9D-44D0-8CA5-941DB051B163}"),
                new Guid("{78D008DF-F7E8-4F99-92A1-42E7AE6E34C3}"),//кемин
                new Guid("{AA466C30-47A6-4252-989D-F44C91A120A5}"),//чуй
                new Guid("{CAE58A37-FBE8-49AA-8E09-B167F871A5E8}"),//город токмок
                new Guid("{BEC45992-2D1C-4488-A260-FC7C5511F619}"),//ыссык-ата
                new Guid("{822E0311-F790-474D-BC97-E67CE8D14009}"),//аламудун
                new Guid("{C139D4DE-9F64-46D2-A908-1C477B78ECCD}"),//сокулук
                new Guid("{CA59DB1A-4AB2-4CB2-90C6-BF323E82E44A}"),//москва
                new Guid("{6612E6F7-BA2B-4E33-ADB9-FA0B14705E27}"),//жайыл
                new Guid("{72075FFB-371D-4C7B-8A5A-993E1C51CBFF}"),//панфилов
            };
            public static List<Guid> talasList = new List<Guid>//Таласская область
            {                                   
            //    new Guid("{BC710978-8C0B-4063-8BA9-A12F75DE4829}"),
                new Guid("{02992EB8-E107-408E-954E-E322A73B433A}"),//г талас
                new Guid("{1E5868C3-B522-4DBF-BC8F-02C75A899C0F}"),//талас рн
                new Guid("{C285A1F2-2114-4752-A626-A235C6D6F2B1}"),//бакай-ата
                new Guid("{17A6CA38-8B7B-4F5C-973B-18F31646CC03}"),//кара-буура
                new Guid("{376FF578-372D-43A4-818E-31BC64327BEF}"),//манас
            };
            public static List<Guid> issyk_kulList = new List<Guid>//Иссык-Кульская область
            {
            //    new Guid("{AD009808-FBAC-43CA-8671-5E790C213497}"),
                new Guid("{43842F64-7BB7-45EC-930B-54AD19186382}"),//каракол
                new Guid("{2FEA2DF5-24F6-4E53-BF6C-22604AC014C1}"),//балыкчы
                new Guid("{416366EE-580B-4E57-8641-0D90A2F5AB73}"),//иссык-куль
                new Guid("{5B438995-2070-46A8-9377-E922F0D64E4F}"),//тюп
                new Guid("{AAD91EAB-297F-40C8-B592-68573495EEC0}"),//жети-огуз
                new Guid("{55F9FFBF-B789-4002-BD64-B04163806897}"),//тон
                new Guid("{D2EDB83E-7EE5-4B5A-82A6-E9E49DD1E3B3}"),//ак-суу
            };
            public static List<Guid> narynList = new List<Guid>//Нарынская область
            {
            //    new Guid("{CF4CA271-9202-4A5A-AE49-EB6237C33982}"),
                new Guid("{CB788BA9-8F94-4317-A655-B4F5527F3A4B}"),//акталаа
                new Guid("{4C60A2A2-73B4-4EA4-A6D0-E2B41370C7FC}"),//ат-баши
                new Guid("{52568E10-0210-4B09-B106-9E14520E26F1}"),//жумгал
                new Guid("{B631F3B0-1656-49C5-9152-0252F304D29B}"),//кочкор
                new Guid("{A6B9B397-81CF-43ED-A17B-6A13C110A678}"),//нарын район
                new Guid("{0A409F28-4A73-4CED-B368-10C6BE53419F}"),//нарын г
            };
            public static List<Guid> batkenList = new List<Guid>//Баткенская область
            {
            //    new Guid("{4E98AD82-8ED4-4727-8E24-678921AB534C}"),
                new Guid("{0BCED873-4950-449A-98E6-69C44F64D70C}"),//баткен рн
                new Guid("{20745158-EAE3-434C-BDF5-5F893C8963ED}"),//баткен г
                new Guid("{E062B2A3-42C4-4619-B086-705E6C5367D3}"),//кадамжай
                new Guid("{F6CECF6A-2D5D-44C1-BC2F-29A836965531}"),//лейлек
                new Guid("{DDE31E57-4454-44EE-AFA9-80227E3C8620}"),//кызыл-кия
                new Guid("{E883C8F3-5E3C-4086-A078-91FD22E6550A}"),//сулюкта
            };
            public static List<Guid> oshList = new List<Guid>//Ошская область
            {
            //    new Guid("{E01E9890-2A41-454E-9EC1-3D4AAF50ED62}"),
                new Guid("{C3C63817-FFB3-4905-BC70-8C3A3DBB67DC}"),//араван
                new Guid("{2A0481C0-FB12-4048-9153-CB7AE997A26C}"),//каракулджа
                new Guid("{3A305A9A-D30E-4C38-9E41-09DF993A658E}"),//кара-суу
                new Guid("{E0AF1DF7-AA64-45DD-873E-510CD413AD35}"),//ноокат
                new Guid("{B0B7490F-E48D-4DC7-9548-0505D73C858F}"),//узген рн
                new Guid("{2F8C280E-6C97-41D5-B24F-7D0DF4FF4C0E}"),//узген г
                new Guid("{5903E184-79E4-4630-AD91-FD488B84B832}"),//алай
                new Guid("{8A7D0C1C-A4A7-4582-8D65-8023CDB273E1}")//чон-алай
            };
            public static List<Guid> osh = new List<Guid>
            {
                new Guid ("{A99E469A-E8D4-4139-B89A-CE4AF6AA0733}")
            };
            public static List<Guid> jalal_abad = new List<Guid>//Джалал-Абадская область
            {
            //    new Guid("{3ED50EFB-3E20-407C-93CB-9D8E0EF15B1B}"),
                new Guid("{D319DC07-E7F7-4997-AFCD-17CEAD707B7F}"),//г джалалабад
                new Guid("{8CAC9C70-D770-4AAB-8B0B-E1C432F6985A}"),//кок-жангак
                new Guid("{EFA2A70A-D10E-4941-B1F8-AE5218E5AD29}"),//майлысуу
                new Guid("{8E3CA117-0956-4F67-A8EF-B8222DC0A21E}"),//ташкумыр
                new Guid("{CB86E66E-8A41-4D92-9FD6-9BCF7E61F543}"),//каракуль
                new Guid("{9375DF49-1B41-4EFD-BB79-0FC2A86ABED6}"),//сузак
                new Guid("{A23770C6-2843-446E-8382-8C50AFD699E5}"),//базар-коргон
                new Guid("{A3E2A017-96F2-47C6-AF09-6FA4287E6953}"),//ноокен
                new Guid("{A5B70AEE-4E8B-4194-864D-F58E5BAB76E7}"),//аксы
                new Guid("{915D0BEB-53C1-440F-9CBA-00612369FEFC}"),//ала-бука
                new Guid("{5026031D-2BC9-4A24-8DB5-30C7DD992352}"),//чаткал
                new Guid("{F9D53DFB-0E18-44E6-A905-8B67F5751D37}"),//токтогул рн
                new Guid("{BF0A92AD-140E-4582-AB9F-DB304890C35A}")//тогуз-торо
            };
        }

        public static class Report_Application4
        {
            static Guid balagaSuyunchuApp = new Guid("{4B8EA3F0-D536-4A59-9D2A-41872D63103C}");
            static Guid payable_state = new Guid("{3DB4DB00-3A7F-4228-A9A3-A413B85C18B4}");

            private static readonly Guid applicantId = new Guid("{6F5B8A06-361E-4559-8A53-9CB480A9B16C}");
            private static readonly Guid childId = new Guid("{6F5B8A06-361E-4559-8A53-9CB480A9B16C}");
            private static readonly Guid bankDetailsId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");  
            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {
                var items = new List<ReportItem>();
                var fd = new DateTime(year, month, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var qb = new QueryBuilder(balagaSuyunchuApp, context.UserId);

                qb.And("&State")
                   .In(new object[]
                   {
                      payable_state
                   })
                   .And("DocDate").Ge(fd).And("DocDate").Le(ld);

                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                var applicantSrc = query.JoinSource(query.Source, applicantId, SqlSourceJoinType.Inner, "Applicant");
                var childSrc = query.JoinSource(query.Source, childId, SqlSourceJoinType.Inner, "Child");
                var bankSrc = query.JoinSource(query.Source, bankDetailsId, SqlSourceJoinType.Inner, "Bank");
                query.AddAttribute(query.Source, "GovUnit");
                query.AddAttribute(applicantSrc, "FirstName");
                query.AddAttribute(applicantSrc, "LastName");
                query.AddAttribute(applicantSrc, "MiddleName");
                query.AddAttribute(applicantSrc, "PIN");
                query.AddAttribute(query.Source, "TelMob");
                query.AddAttribute(applicantSrc, "PassportNo");
                query.AddAttribute(childSrc, "PassportNo");
                query.AddAttribute(bankSrc, "Name");
                query.AddAttribute(applicantSrc, "&Id");
                query.AddAttribute(query.Source, "ZagsId");

                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        var orgName = "-";
                        if (!reader.IsDbNull(0)) 
                            {
                            try
                            {
                                orgName=reader.GetString(0);
                            }
                            catch
                            {
                                orgName = reader.GetGuid(0).ToString();
                            }
                            };
                        var firstNameApplicant = reader.IsDbNull(1) ? "-" : reader.GetString(1);
                        var lastNameApplicant = reader.IsDbNull(2) ? "-" : reader.GetString(2);
                        var middleNameApplicant = reader.IsDbNull(3) ? "-" : reader.GetString(3);
                        var pin = reader.IsDbNull(4) ? "-" : reader.GetString(4);
                        var phone = reader.IsDbNull(5) ? "-" : reader.GetString(5); 
                        var applicantPassportNo = reader.IsDbNull(6) ? "-" : reader.GetString(6);
                        var childPassportNo = reader.IsDbNull(7) ? "-" : reader.GetString(7);
                        var bankName = reader.IsDbNull(8) ? "-" : reader.GetString(8);
                        var applicantId = reader.IsDbNull(9) ? Guid.Empty : reader.GetGuid(9);
                        var zagsId= reader.IsDbNull(10) ? 0 : reader.GetInt32(10);

                        var item = GetReportItem(items, applicantId, orgName, zagsId);
                        item.FirstName = firstNameApplicant;
                        item.LastName = lastNameApplicant;
                        item.MiddleName = middleNameApplicant;
                        item.Pin = pin;
                        item.Phone = phone;
                        item.BankName = bankName;
                        item.PassportNumber = applicantPassportNo;
                        item.CertificateNumber = childPassportNo;

                        if (bishkekList.Contains(item.getZagsId())) item.RegionName = "г.Бишкек";
                        if (chuiList.Contains(item.getZagsId())) item.RegionName = "Чуйская область";
                        if (talasList.Contains(item.getZagsId())) item.RegionName = "Таласская область";
                        if (issyk_kulList.Contains(item.getZagsId())) item.RegionName = "Иссык-Кульская область";
                        if (narynList.Contains(item.getZagsId())) item.RegionName = "Нарынская область";
                        if (batkenList.Contains(item.getZagsId())) item.RegionName = "Баткенская область";
                        if (oshList.Contains(item.getZagsId())) item.RegionName = "Ошская область";
                        if (osh.Contains(item.getZagsId())) item.RegionName = "г.Ош";
                        if (jalal_abad.Contains(item.getZagsId())) item.RegionName = "Джалал-Абадская область";
                    }
                }

                return items;
            }


        static ReportItem GetReportItem(List<ReportItem> items, Guid _applicantId, string districtName, int _zagsId)
        {
            var item = items.FirstOrDefault(x => x.getApplicantId().Equals(_applicantId));
            if (item != null) return item;
                item = new ReportItem(_applicantId, districtName, _zagsId);
            items.Add(item);
            return item;
        }
        [DataContract]
        public class ReportItem
        {

            private Guid applicantId { get; set; }
            [DataMember]
            public string RegionName { get; set; }
            [DataMember]
            public string DistrictName { get; set; }
                [DataMember]
            public string FirstName { get; set; }
                [DataMember]
            public string LastName { get; set; }
                [DataMember]
            public string MiddleName { get; set; }
                [DataMember]
            public string Pin { get; set; }
                [DataMember]
            public string Phone { get; set; }
                [DataMember]
            public string CertificateNumber { get; set; }
                [DataMember]
            public string PassportNumber { get; set; }
                [DataMember]
            public string BankName { get; set; }
            
            private int ZagsId { get; set; }

            public ReportItem(Guid _applicantId, string districtName, int _zagsId)
            {
                applicantId = _applicantId;
                DistrictName = districtName;
                ZagsId=_zagsId;
            }

            public Guid getApplicantId()
            {
                return applicantId;
            }
            
            public int getZagsId()
            {
                return ZagsId;
            }
            }

            public static List<int> bishkekList = new List<int>//г.Бишкек
            {                                
                9993,//ленин
                9991,//первомайский
                9992,//свердлово
                9994,//октябрь
                9995, //Дворец бракосочетания г.Бишкек
                9990,//Управление актов гражданского состояния
                9996,//Отдел ЗАГС гражданского обслуживания г.Бишкек
                29260968,//Сектор выездной регистрации г.Бишкек
            };
            public static List<int> chuiList = new List<int>//Чуйская область
            {
                9997,//кемин
                9998,//чуй
                9999,//ыссык-ата
                10001,//аламудун
                10002,//сокулук
                10000,//москва
                10004,//жайыл
                10003,//панфилов
                35926975,//c. Ивановка ОЗАГС Ысык-Атинского района
                36812923,//отдел ЗАГС г.Кант
                37278107,//отдел ЗАГС г.Кант
            };
            public static List<int> talasList = new List<int>//Таласская область
            {                                   
                34592703,//талас рн
                10047,//бакай-ата
                10048,//кара-буура
                10046,//манас
            };
            public static List<int> issyk_kulList = new List<int>//Иссык-Кульская область
            {
                10037,//каракол
                10038,//балыкчы
                10039,//иссык-куль
                10041,//тюп
                10042,//жети-огуз
                10043,//тон
                10040//ак-суу
            };
            public static List<int> narynList = new List<int>//Нарынская область
            {
                10006,//акталаа
                10008,//ат-баши
                10007,//жумгал
                10009,//кочкор
                10005 ////нарын район
            };
            public static List<int> batkenList = new List<int>//Баткенская область
            {
                10032,//баткен рн
                10034,//кадамжай
                10033,//лейлек
                10035,//кызыл-кия
                10036//сулюкта
            };
            public static List<int> oshList = new List<int>//Ошская область
            {
                10026,//араван
                10028,//каракулджа
                10027,//кара-суу
                10029,//ноокат
                10030,//узген рн
                10025,//алай
                10031,//чон-алай
                37851512,//отдел ЗАГС Фрунзенского района Ошской области
                38829108,//Дом бракосочетания г.Фрунзе
                40748164 //ПО с.1-Май Ала-Букинского района
            };
            public static List<int> osh = new List<int>
            {
                10024,
                29787977//Сектор выездной регистрации г.Ош
            };
            public static List<int> jalal_abad = new List<int>//Джалал-Абадская область
            {
                10010,//г джалалабад
                34599769,//майлысуу,ноокен
                10012,//ташкумыр
                10013,//каракуль
                10018,//сузак
                13328252, //кок-жангакский зональный
                10022, //кок-артский зональный
                10016,//базар-коргон
                10015,//аксы
                10014,//ала-бука
                10021,//чаткал
                10019,//токтогул рн
                10023,//уч терек зональный
                10020//тогуз-торо
            };
        }
        public static class Report_Application5
        {
            static Guid balagaSuyunchuApp = new Guid("{4B8EA3F0-D536-4A59-9D2A-41872D63103C}");
            static Guid payable_state = new Guid("{3DB4DB00-3A7F-4228-A9A3-A413B85C18B4}");

            private static readonly Guid applicantId = new Guid("{6F5B8A06-361E-4559-8A53-9CB480A9B16C}");
            private static readonly Guid childId = new Guid("{6F5B8A06-361E-4559-8A53-9CB480A9B16C}");
            private static readonly Guid bankDetailsId = new Guid("{F21733AC-AC34-499B-B0CC-F47C8053D6B7}");

            private static readonly decimal BALAGA_SUYUNCHU_PRICE= 4000;

            public static HashSet<ZAGSInfo> PilotZAGS = new HashSet<ZAGSInfo>
        {
            new ZAGSInfo
            {
                Id = 10001,
                Name = "Отдел ЗАГС Аламудунского района",
                RegionName="г.Бишкек"
            },
            new ZAGSInfo
            {
                Id = 10002,
                Name = "Отдел ЗАГС Сокулукского района",
                RegionName="Чуйская область"
            },
            new ZAGSInfo
            {
                Id = 9992,
                Name = "Отдел ЗАГС Свердловского района, г .Бишкек",
                RegionName="г.Бишкек"

            },
            new ZAGSInfo
            {
                Id = 9991,
                Name = "Отдел ЗАГС Первомайского района, г.Бишкек",
                RegionName="г.Бишкек"
            },
            new ZAGSInfo
            {
                Id = 9993,
                Name = "Отдел ЗАГС Ленинского района, г.Бишкек",
                RegionName="г.Бишкек"
            },
            new ZAGSInfo
            {
                Id = 9994,
                Name = "Отдел ЗАГС Октябрьского района, г. Бишкек",
                RegionName="г.Бишкек"
            },
            new ZAGSInfo
            {
                Id = 9995,
                Name = "Дворец бракосочетания г.Бишкек",
                RegionName="г.Бишкек"
            },
            new ZAGSInfo
            {
                Id = 9997,
                Name = "Отдел ЗАГС Кеминского района",
                RegionName="Чуйская область"
            },
            new ZAGSInfo
            {
                Id = 9998,
                Name = "Отдел ЗАГС Чуйского района",
                RegionName="Чуйская область"
            },
            new ZAGSInfo
            {
                Id = 9999,
                Name = "Отдел ЗАГС Ысык-Атинского района",
                RegionName="Чуйская область"
            },
            new ZAGSInfo
            {
                Id = 10000,
                Name = "Отдел ЗАГС Московского района",
                RegionName="Чуйская область"
            },
            new ZAGSInfo
            {
                Id = 10003,
                Name = "Отдел ЗАГС Панфиловского района",
                RegionName="Чуйская область"
            },
            new ZAGSInfo
            {
                Id = 10004,
                Name = "Отдел ЗАГС Жайылского района",
                RegionName="Чуйская область"
            },
            new ZAGSInfo
            {
                Id = 10005,
                Name = "Отдел ЗАГС г. Нарын",
                RegionName="Нарынская область"
            },
            new ZAGSInfo
            {
                Id = 10006,
                Name = "Отдел ЗАГС Ак-Талинского района",
                RegionName="Нарынская область"
            },
            new ZAGSInfo
            {
                Id = 10007,
                Name = "Отдел ЗАГС Жумгальского района",
                RegionName="Нарынская область"
            },
            new ZAGSInfo
            {
                Id = 10008,
                Name = "Отдел ЗАГС Ат-Башинского района",
                RegionName="Нарынская область"
            },
            new ZAGSInfo
            {
                Id = 10009,
                Name = "Отдел ЗАГС Кочкорского района",
                RegionName="Нарынская область"
            },
            new ZAGSInfo
            {
                Id = 10010,
                Name = "Отдел ЗАГС г. Жалал-Абад",
                RegionName="Джалал-Абадская область"
            },
            new ZAGSInfo
            {
                Id = 10011,
                Name = "Отдел ЗАГС г. Майлуу-Суу",
                RegionName="Джалал-Абадская область"
            },
            new ZAGSInfo
            {
                Id = 10012,
                Name = "Отдел ЗАГС г. Таш-Кумыр",
                RegionName="Джалал-Абадская область"
            },
            new ZAGSInfo
            {
                Id = 10013,
                Name = "Отдел ЗАГС г. Кара-Куль",
                RegionName="Джалал-Абадская область"
            },
            new ZAGSInfo
            {
                Id = 10014,
                Name = "Отдел ЗАГС Ала-Букинского района",
                RegionName="Джалал-Абадская область"
            },
            new ZAGSInfo
            {
                Id = 10015,
                Name = "Отдел ЗАГС Аксыйского района",
                RegionName="Джалал-Абадская область"
            },
            new ZAGSInfo
            {
                Id = 10016,
                Name = "Отдел ЗАГС Базар-Коргонского района",
                RegionName="Джалал-Абадская область"
            },
            new ZAGSInfo
            {
                Id = 10019,
                Name = "Отдел ЗАГС Токтогульского района",
                RegionName="Джалал-Абадская область"
            },
            new ZAGSInfo
            {
                Id = 10020,
                Name = "Отдел ЗАГС Тогуз-Тороузкого района",
                RegionName="Джалал-Абадская область"
            },
            new ZAGSInfo
            {
                Id = 10021,
                Name = "Отдел ЗАГС Чаткальского района",
                RegionName="Джалал-Абадская область"
            },
            new ZAGSInfo
            {
                Id = 10022,
                Name = "Кок-Артский зональный ЗАГС в Сузакский район",
                RegionName="Джалал-Абадская область"
            },
            new ZAGSInfo
            {
                Id = 10023,
                Name = "Уч-Терекский зональный ЗАГС в Токтогульский район",
                RegionName="Джалал-Абадская область"
            },
            new ZAGSInfo
            {
                Id = 10024,
                Name = "Отдел ЗАГС г. Ош",
                RegionName="г.Ош"
            },
            new ZAGSInfo
            {
                Id = 10025,
                Name = "Отдел ЗАГС Алайского района",
                RegionName="Ошская область"
            },
            new ZAGSInfo
            {
                Id = 10026,
                Name = "Отдел ЗАГС Араванского района",
                RegionName="Ошская область"
            },
            new ZAGSInfo
            {
                Id = 10027,
                Name = "Отдел ЗАГС Кара-Сууйского района",
                RegionName="Ошская область"
            },
            new ZAGSInfo
            {
                Id = 10028,
                Name = "Отдел ЗАГС Кара-Кульжинского района",
                RegionName="Ошская область"
            },
            new ZAGSInfo
            {
                Id = 10029,
                Name = "Отдел ЗАГС Ноокатского района",
                RegionName="Ошская область"
            },
            new ZAGSInfo
            {
                Id = 10030,
                Name = "Отдел ЗАГС Узгенского района",
                RegionName="Ошская область"
            },
            new ZAGSInfo
            {
                Id = 10031,
                Name = "Отдел ЗАГС Чон - Алайского района",
                RegionName="Ошская область"
            },
            new ZAGSInfo
            {
                Id = 10032,
                Name = "Отдел ЗАГС Баткенского района",
                RegionName="Баткенская область"
            },
            new ZAGSInfo
            {
                Id = 10033,
                Name = "Отдел ЗАГС Лейлекского района",
                RegionName="Баткенская область"
            },
            new ZAGSInfo
            {
                Id = 10035,
                Name = "Отдел ЗАГС  г. Кызыл-Кия",
                RegionName="Баткенская область"
            },
            new ZAGSInfo
            {
                Id = 10036,
                Name = "Отдел ЗАГС г. Сулюкта",
                RegionName="Баткенская область"
            },
            new ZAGSInfo
            {
                Id = 10037,
                Name = "Отдел ЗАГС г. Каракол",
                RegionName="Иссык-Кульская область"
            },
            new ZAGSInfo
            {
                Id = 10038,
                Name = "Отдел ЗАГС г. Балыкчы",
                RegionName="Иссык-Кульская область"
            },
            new ZAGSInfo
            {
                Id = 10039,
                Name = "Отдел ЗАГС Иссык-Кульского района",
                RegionName="Иссык-Кульская область"
            },
            new ZAGSInfo
            {
                Id = 10040,
                Name = "Отдел ЗАГС Ак-Суйского района",
                RegionName="Иссык-Кульская область"
            },
            new ZAGSInfo
            {
                Id = 10041,
                Name = "Отдел ЗАГС Тюпского района",
                RegionName="Иссык-Кульская область"
            },
            new ZAGSInfo
            {
                Id = 10042,
                Name = "Отдел ЗАГС Жети-Огузского района",
                RegionName="Иссык-Кульская область"
            },
            new ZAGSInfo
            {
                Id = 10043,
                Name = "Отдел ЗАГС Тонского района",
                RegionName="Иссык-Кульская область"
            },
            new ZAGSInfo
            {
                Id = 10044,
                Name = "Отдел ЗАГС г. Талас",
                RegionName="Таласская область"
            },
            new ZAGSInfo
            {
                Id = 10045,
                Name = "Отдел ЗАГС Таласского района",
                RegionName="Таласская область"
            },
            new ZAGSInfo
            {
                Id = 10046,
                Name = "Отдел ЗАГС Манасского района",
                RegionName="Таласская область"
            },
            new ZAGSInfo
            {
                Id = 10047,
                Name = "Отдел ЗАГС Бакай-Атинского района",
                RegionName="Таласская область"
            },
            new ZAGSInfo
            {
                Id = 9990,
                Name = "Управление актов гражданского состояния",
                RegionName="г.Бишкек"
            },
            new ZAGSInfo
            {
                Id = 9996,
                Name = "Отдел ЗАГС гражданского обслуживания г.Бишкек",
                RegionName="г.Бишкек"
            },
            new ZAGSInfo
            {
                Id = 10017,
                Name = "Отдел ЗАГС Ноокенского района",
                RegionName="Джалал-Абадская область"
            },
            new ZAGSInfo
            {
                Id = 10018,
                Name = "Отдел ЗАГС Сузакского района",
                RegionName="Джалал-Абадская область"
            },
            new ZAGSInfo
            {
                Id = 10034,
                Name = "Отдел ЗАГС Кадамжайского района",
                RegionName="Баткенская область"
            },
            new ZAGSInfo
            {
                Id = 10048,
                Name = "Отдел ЗАГС Кара-Бууринского района",
                RegionName="Таласская область"
            },
            new ZAGSInfo
            {
                Id = 13328252,
                Name = "Кок-Жангакский зональный ЗАГС в Сузакский район",
                RegionName="Джалал-Абадская область"
            },
            new ZAGSInfo
            {
                Id = 29260968,
                Name = "Сектор выездной регистрации г.Бишкек",
                RegionName="г.Бишкек"
            },
            new ZAGSInfo
            {
                Id = 29787977,
                Name = "Сектор выездной регистрации г.Ош",
                RegionName="г.Ош"
            },
            new ZAGSInfo
            {
                Id = 34592703,
                Name = "Отдел ЗАГС г. Талас и Таласского района",
                RegionName="Таласская область"
            },
            new ZAGSInfo
            {
                Id = 34599769,
                Name = "Отдел ЗАГС г. Майлуу-Суу и Ноокенского района",
                RegionName="Джалал-Абадская область"
            },
            new ZAGSInfo
            {
                Id = 35926975,
                Name = "c. Ивановка ОЗАГС Ысык-Атинского района",
                RegionName="Чуйская область"
            },
            new ZAGSInfo
            {
                Id = 36812923,
                Name = "отдел ЗАГС г.Кант",
                RegionName="Чуйская область"
            },
            new ZAGSInfo
            {
                Id = 37278107,
                Name = "отдел ЗАГС г.Кант",
                RegionName="Чуйская область"
            },
            new ZAGSInfo
            {
                Id = 37598780,
                Name = "Отдел ЗАГС Аламудунского района",
                RegionName="Чуйская область"
            },
            new ZAGSInfo
            {
                Id = 37851512,
                Name = "отдел ЗАГС Фрунзенского района Ошской области",
                RegionName="Ошская область"
            },
            new ZAGSInfo
            {
                Id = 38829108,
                Name = "Дом бракосочетания г.Фрунзе",
                RegionName="Ошская область"
            },
            new ZAGSInfo
            {
                Id = 40748164,
                Name = "ПО с.1-Май Ала-Букинского района",
                RegionName="Джалал-Абадская область"
            }
        };
            public class ZAGSInfo
            {
                public int Id { get; set; }
                public string Name { get; set; }
                public string RegionName { get; set; }
            }
            public static List<ReportItem> Execute(WorkflowContext context, int year, int month)
            {
                var items = new List<ReportItem>();
                var fd = new DateTime(year, 1, 1);
                var ld = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                var qb = new QueryBuilder(balagaSuyunchuApp, context.UserId);

                qb.And("&State")
                   .In(new object[]
                   {
                      payable_state
                   })
                   .And("DocDate").Ge(fd).And("DocDate").Le(ld);

                SqlQuery query = SqlQueryBuilder.Build(context.DataContext, qb.Def);
                query.AddAttribute("GovUnit", SqlQuerySummaryFunction.Count);
                query.AddAttribute("ZagsId");
                query.AddAttribute("DocDate");
                query.AddGroupAttribute("ZagsId");
                query.AddGroupAttribute("DocDate");


                using (var reader = new SqlQueryReader(context.DataContext, query))
                {
                    while (reader.Read())
                    {
                        var govUnit = reader.IsDbNull(0) ? 0 : reader.GetInt32(0);
                        var zagsId = reader.IsDbNull(1) ? 0 : reader.GetInt32(1);
                        var docDate = reader.IsDbNull(2) ? DateTime.MinValue : reader.GetDateTime(2);
                        var pilotZags = PilotZAGS.Where(x => x.Id == zagsId).FirstOrDefault();
                        var item = GetReportItem(items, pilotZags.Name, pilotZags.RegionName, zagsId);
                        int indexOfMonth = docDate.Month;
                        switch(indexOfMonth)
                        {
                            case 1:item.January.Count += govUnit;
                                item.January.Sum = item.January.Count * BALAGA_SUYUNCHU_PRICE;
                                break;
                            case 2:item.February.Count += govUnit;
                                item.February.Sum = item.February.Count * BALAGA_SUYUNCHU_PRICE;
                                break;
                            case 3:
                                item.March.Count += govUnit;
                                item.March.Sum = item.March.Count * BALAGA_SUYUNCHU_PRICE;
                                break;
                            case 4:
                                item.April.Count += govUnit;
                                item.April.Sum = item.April.Count * BALAGA_SUYUNCHU_PRICE;
                                break;
                            case 5:
                                item.May.Count += govUnit;
                                item.May.Sum = item.May.Count * BALAGA_SUYUNCHU_PRICE;
                                break;
                            case 6:
                                item.June.Count += govUnit;
                                item.June.Sum = item.June.Count * BALAGA_SUYUNCHU_PRICE;
                                break;
                            case 7:
                                item.July.Count += govUnit;
                                item.July.Sum = item.July.Count * BALAGA_SUYUNCHU_PRICE;
                                break;
                            case 8:
                                item.August.Count += govUnit;
                                item.August.Sum = item.August.Count * BALAGA_SUYUNCHU_PRICE;
                                break;
                            case 9:
                                item.September.Count += govUnit;
                                item.September.Sum = item.September.Count * BALAGA_SUYUNCHU_PRICE;
                                break;
                            case 10:
                                item.October.Count += govUnit;
                                item.October.Sum = item.October.Count * BALAGA_SUYUNCHU_PRICE;
                                break;
                            case 11:
                                item.November.Count += govUnit;
                                item.November.Sum = item.November.Count * BALAGA_SUYUNCHU_PRICE;
                                break;
                            case 12:
                                item.December.Count += govUnit;
                                item.December.Sum = item.December.Count * BALAGA_SUYUNCHU_PRICE;
                                break;
                        }

                    }
                }
               
                return items;
            }


            static ReportItem GetReportItem(List<ReportItem> items, string districtName,string regionName, int _zagsId)
            {
                var item = items.FirstOrDefault(x => x.getZagsId().Equals(_zagsId));
                if (item != null) return item;
                item = new ReportItem(districtName, regionName, _zagsId);
                items.Add(item);
                return item;
            }
            [DataContract]
            public class ReportItem
            {

                [DataMember]
                public string RegionName { get; set; }
                [DataMember]
                public string DistrictName { get; set; }

                private int ZagsId { get; set; }

                [DataMember]
                public MonthReport January { get; set; }
                [DataMember]
                public MonthReport February { get; set; }
                [DataMember]
                public MonthReport March { get; set; }
                [DataMember]
                public MonthReport April { get; set; }
                [DataMember]
                public MonthReport May { get; set; }
                [DataMember]
                public MonthReport June { get; set; }
                [DataMember]
                public MonthReport July { get; set; }
                [DataMember]
                public MonthReport August { get; set; }
                [DataMember]
                public MonthReport September { get; set; }
                [DataMember]
                public MonthReport October { get; set; }
                [DataMember]
                public MonthReport November { get; set; }
                [DataMember]
                public MonthReport December { get; set; }

                public ReportItem(string districtName,string regionName, int _zagsId)
                {
                    DistrictName = districtName;
                    RegionName = regionName;
                    ZagsId = _zagsId;
                    January = new MonthReport();
                    February = new MonthReport();
                    March = new MonthReport();
                    April = new MonthReport();
                    May = new MonthReport();
                    June = new MonthReport();
                    July = new MonthReport();
                    August = new MonthReport();
                    September = new MonthReport();
                    October = new MonthReport();
                    November = new MonthReport();
                    December = new MonthReport();
                }


                [DataContract]
                public class MonthReport
                {
                    [DataMember]
                    public int Count { get; set; }
                    [DataMember]
                    public decimal Sum { get; set; }
                }

                public int getZagsId()
                {
                    return ZagsId;
                }
            }

     
        }
    }
}
