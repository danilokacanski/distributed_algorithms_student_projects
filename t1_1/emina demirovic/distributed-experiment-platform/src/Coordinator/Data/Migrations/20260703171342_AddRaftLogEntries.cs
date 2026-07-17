using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Coordinator.Data.Migrations
{
    /// <inheritdoc />
    public partial class AddRaftLogEntries : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "RaftLogEntries",
                columns: table => new
                {
                    NodeId = table.Column<string>(type: "TEXT", maxLength: 100, nullable: false),
                    LogIndex = table.Column<long>(type: "INTEGER", nullable: false),
                    Term = table.Column<long>(type: "INTEGER", nullable: false),
                    CommandId = table.Column<Guid>(type: "TEXT", nullable: false),
                    CommandType = table.Column<string>(type: "TEXT", maxLength: 100, nullable: false),
                    CommandPayloadJson = table.Column<string>(type: "TEXT", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_RaftLogEntries", x => new { x.NodeId, x.LogIndex });
                });

            migrationBuilder.CreateIndex(
                name: "IX_RaftLogEntries_NodeId_CommandId",
                table: "RaftLogEntries",
                columns: new[] { "NodeId", "CommandId" },
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "RaftLogEntries");
        }
    }
}
