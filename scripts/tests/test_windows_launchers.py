import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class WindowsLauncherContractTest(unittest.TestCase):
    def read(self, relative):
        return (ROOT / relative).read_text(encoding="utf-8")

    def test_main_launcher_is_autonomous_and_preserves_exit_code(self):
        launcher = self.read("RUN-KAFKA-LAB.bat").lower()
        self.assertIn("docker compose version", launcher)
        self.assertIn("docker desktop.exe", launcher)
        self.assertIn("for /l %%i in (1,1,60)", launcher)
        self.assertIn("timeout /t 2", launcher)
        self.assertIn("docker compose up -d --build", launcher)
        self.assertIn('call "scripts\\run-benchmark.bat" 1000000', launcher)
        self.assertIn("exit /b %rc%", launcher)
        self.assertNotIn("claude", launcher)
        self.assertNotIn("dangerously-skip-permissions", launcher)

    def test_git_bash_paths_are_quoted_and_acl_is_user_scoped(self):
        wrappers = (
            "scripts/generate.bat", "scripts/run-benchmark.bat",
            "scripts/run-full-suite.bat", "scripts/run-stress.bat", "scripts/validate.bat",
        )
        for relative in wrappers:
            content = self.read(relative)
            self.assertIn('"%BASH%" "%~dp0', content, relative)
        for relative in ("RUN-KAFKA-LAB.bat", "scripts/start.bat",
                         "scripts/run-benchmark.bat", "scripts/run-full-suite.bat",
                         "scripts/run-stress.bat"):
            content = self.read(relative)
            self.assertIn("%USERDOMAIN%\\%USERNAME%", content, relative)
            self.assertNotIn("S-1-1-0", content, relative)


if __name__ == "__main__":
    unittest.main()
