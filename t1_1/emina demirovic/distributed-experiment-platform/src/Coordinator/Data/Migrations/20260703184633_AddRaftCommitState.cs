using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Coordinator.Data.Migrations
{
    /// <inheritdoc />
    public partial class AddRaftCommitState : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "RaftCommitStates",
                columns: table => new
                {
                    NodeId = table.Column<string>(type: "TEXT", maxLength: 100, nullable: false),
                    CommitIndex = table.Column<long>(type: "INTEGER", nullable: false),
                    LastApplied = table.Column<long>(type: "INTEGER", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_RaftCommitStates", x => x.NodeId);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "RaftCommitStates");
        }
    }
}
